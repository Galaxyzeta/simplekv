package cache

type Cacher interface {
	Load(key string) (interface{}, bool)
	Store(key string, value interface{})
	Delete(key string)
}
