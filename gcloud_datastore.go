/*

A Google App Engine Datastore session store implementation.

*/

package session

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"context"
	"log"
)

type Codec struct {
	Marshal   func(interface{}) ([]byte, error)
	Unmarshal func([]byte, interface{}) error
}

var (
	// JSON is a Codec that uses the json package.
	JSON = Codec{json.Marshal, json.Unmarshal}
)

// A Google App Engine Memcache session store implementation.
type dataStore struct {
	keyPrefix string // Prefix to use in front of session ids to construct Memcache key
	retries   int    // Number of retries to perform in case of general Memcache failures

	codec *Codec // Codec used to marshal and unmarshal a Session to a byte slice

	dsEntityName string // Name of the datastore entity to use to save sessions

	// Map of sessions (mapped from ID) that were accessed using this store; usually it will only be 1.
	// It is also used as a cache, should the user call Get() with the same id multiple times.
	sessions map[string]Session

	projectId string

	mux *sync.RWMutex
	// mutex to synchronize access to sessions
}

// DataStoreOptions defines options that may be passed when creating a new Memcache session store.
// All fields are optional; default value will be used for any field that has the zero value.
type DataStoreOptions struct {
	// Prefix to use when storing sessions in the Memcache, cannot contain a null byte
	// and cannot be longer than 250 chars (bytes) when concatenated with the session id; default value is the empty string
	// The Memcache key will be this prefix and the session id concatenated.
	KeyPrefix string

	// Number of retries to perform if Memcache operations fail due to general service error;
	// default value is 3
	Retries int

	// Codec used to marshal and unmarshal a Session to a byte slice;
	// Default value is TODO
	Codec *Codec

	// Name of the entity to use for saving sessions;
	// default value is "sess_"
	// Not used if OnlyMemcache=true.
	DSEntityName string

	// Project ID leave empty and it will auto populate for environment variables
	ProjectId string
}

// SessEntity models the session entity saved to Datastore.
// The Key is the session id.
type SessEntity struct {
	Expires time.Time `datastore:"exp"`
	Value   []byte    `datastore:"val"`
}

// Pointer to zero value of DataStoreOptions to be reused for efficiency.
var zeroDataStoreOptions = new(DataStoreOptions)

// NewDataStore returns a new, GAE Memcache session Store with default options.
// Default values of options are listed in the DataStoreOptions type.
//
// Important! Since accessing the Memcache relies on Appengine Context
// which is bound to an http.Request, the returned Store can only be used for the lifetime of a request!
func NewDataStore() Store {
	return NewDataStoreOptions(zeroDataStoreOptions)
}

const defaultDSEntityName = "sess_" // Default value of DSEntityName.

// NewDataStoreOptions returns a new, GAE datastore session Store with the specified options.
func NewDataStoreOptions(o *DataStoreOptions) Store {
	s := &dataStore{
		keyPrefix:    o.KeyPrefix,
		retries:      o.Retries,
		dsEntityName: o.DSEntityName,
		sessions:     make(map[string]Session, 2),
		mux:          &sync.RWMutex{},
		projectId:    o.ProjectId,
	}
	if s.retries <= 0 {
		s.retries = 3
	}
	if o.Codec != nil {
		s.codec = o.Codec
	} else {
		s.codec = &JSON
	}
	if s.dsEntityName == "" {
		s.dsEntityName = defaultDSEntityName
	}
	return s
}

// Get is to implement Store.Get().
// Important! Since sessions are marshalled and stored in the Memcache,
// the mutex of the Session (Session.RWMutex()) will be different for each
// Session value (even though they might have the same session id)!
func (s *dataStore) Get(id string) Session {
	s.mux.RLock()
	defer s.mux.RUnlock()

	// First check our "cache"
	if sess := s.sessions[id]; sess != nil {
		return sess
	}

	// Next check in Memcache
	var err error
	var sess *sessionImpl
	var client *datastore.Client

	client, err = datastore.NewClient(context.Background(), s.projectId)
	if err != nil {
		log.Printf("Error: %v", err)
		return nil
	}

	if sess == nil {
		// Now it's time to check in the Datastore.
		key := datastore.NameKey(s.dsEntityName, id, nil)
		for i := 0; i < s.retries; i++ {
			e := SessEntity{}
			err = client.Get(context.Background(), key, &e)
			if err == datastore.ErrNoSuchEntity {
				log.Printf("Error: %v", err)
				return nil // It's not in the Datastore either
			}
			if err != nil {
				// Service error? Retry..
				log.Printf("Error: %v", err)
				continue
			}
			if e.Expires.Before(time.Now()) {
				// Session expired.
				client.Delete(context.Background(), key) // Omitting error check...
				return nil
			}
			var sess_ sessionImpl
			if err = s.codec.Unmarshal(e.Value, &sess_); err != nil {
				log.Printf("Error: %v", err)
				break // Invalid data in stored session entity...
			}
			sess = &sess_
			break
		}
	}

	if sess == nil {
		log.Printf("ERROR: Failed to get session from datastore, id: %s, error: %v", id, err)
		return nil
	}

	// Yes! We have it!
	// "Actualize" it, but first, Mutex is not marshaled, so create a new one:
	sess.mux = &sync.RWMutex{}
	sess.Access()
	s.sessions[id] = sess
	return sess
}

// Add is to implement Store.Add().
func (s *dataStore) Add(sess Session) {
	s.mux.Lock()
	defer s.mux.Unlock()

	log.Printf("INFO: Session added: %s", sess.ID())
	s.sessions[sess.ID()] = sess
}

// Remove is to implement Store.Remove().
func (s *dataStore) Remove(sess Session) {
	s.mux.Lock()
	defer s.mux.Unlock()

	var err error
	var client *datastore.Client
	client, err = datastore.NewClient(context.Background(), s.projectId)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	key := datastore.NameKey(s.dsEntityName, sess.ID(), nil)
	client.Delete(context.Background(), key)
	log.Printf("ERROR: Failed to remove session from memcache, id: %s, error: %v", sess.ID(), err)
}

// Close is to implement Store.Close().
func (s *dataStore) Close() {
	// Flush out sessions that were accessed from this store. No need locking, we're closing...
	// We could use Codec.SetMulti(), but sessions will contain at most 1 session like all the times.
	s.saveToDatastore()
}

// saveToDatastore saves the sessions of the Store to the Datastore
// in the caller's goroutine.
func (s *dataStore) saveToDatastore() {
	// Save sessions that were accessed from this store. No need locking, we're closing...
	// We could use datastore.PutMulti(), but sessions will contain at most 1 session like all the times.
	client, err := datastore.NewClient(context.Background(), s.projectId)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	for _, sess := range s.sessions {
		value, err := s.codec.Marshal(sess)
		if err != nil {
			log.Printf("ERROR: Failed to marshal session: %s, error: %v", sess.ID(), err)
			continue
		}
		e := SessEntity{
			Expires: sess.Accessed().Add(sess.Timeout()),
			Value:   value,
		}
		key := datastore.NameKey(s.dsEntityName, sess.ID(), nil)
		for i := 0; i < s.retries; i++ {
			if _, err = client.Put(context.Background(), key, &e); err == nil {
				log.Printf("Error: %v", err)
				break
			}
		}
		if err != nil {
			log.Printf("ERROR: Failed to save session to datastore: %s, error: %v", sess.ID(), err)
		}
	}
}

// PurgeExpiredSessFromDSFunc returns a request handler function which deletes expired sessions
// from the Datastore.
// dsEntityName is the name of the entity used for saving sessions; pass an empty string
// to use the default value (which is "sess_").
//
// It is recommended to register the returned handler function to a path which then can be defined
// as a cron job to be called periodically, e.g. in every 30 minutes or so (your choice).
// As cron handlers may run up to 10 minutes, the returned handler will stop at 8 minutes
// to complete safely even if there are more expired, undeleted sessions.
//
// The response of the handler func is a JSON text telling if the handler was able to delete all expired sessions,
// or that it was finished early due to the time. Examle of a respone where all expired sessions were deleted:
//
//     {"completed":true}
func PurgeExpiredSessFromDSFunc(projectId, dsEntityName string) http.HandlerFunc {
	if dsEntityName == "" {
		dsEntityName = defaultDSEntityName
	}

	return func(w http.ResponseWriter, r *http.Request) {
		client, err := datastore.NewClient(context.Background(), projectId)
		if err != nil {
			log.Printf("Error: %v", err)
			return
		}

		// Delete in batches of 100
		q := datastore.NewQuery(dsEntityName).Filter("exp<", time.Now()).KeysOnly().Limit(100)

		deadline := time.Now().Add(time.Minute * 8)

		for {
			var err error
			var keys []*datastore.Key

			if keys, err = client.GetAll(context.Background(), q, nil); err != nil {
				// Datastore error.
				log.Printf("ERROR: Failed to query expired sessions: %v", err)
				http.Error(w, "Failed to query expired sessions!", http.StatusInternalServerError)
			}
			if len(keys) == 0 {
				// We're done, no more expired sessions
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"completed":true}`))
				return
			}

			if err = client.DeleteMulti(context.Background(), keys); err != nil {
				log.Printf("ERROR: Error while deleting expired sessions: %v", err)
			}

			if time.Now().After(deadline) {
				// Our time is up, return
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"completed":false}`))
				return
			}
			// We have time to continue
		}
	}
}
