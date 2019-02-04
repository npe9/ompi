
typedef pthread_key_t opal_tsd_key_t;

static inline int
opal_tsd_key_delete(opal_tsd_key_t key)
{
    return pthread_key_delete(key);
}

static inline int
opal_tsd_setspecific(opal_tsd_key_t key, void *value)
{
    return pthread_setspecific(key, value);
}

static inline int
opal_tsd_getspecific(opal_tsd_key_t key, void **valuep)
{
    *valuep = pthread_getspecific(key);
    return OPAL_SUCCESS;
}