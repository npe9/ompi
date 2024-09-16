/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc. All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/threads/threads.h"
#include "opal/threads/tsd.h"
#include "opal/constants.h"

#include <sys/mman.h>
#include <lithe/fork_join_sched.h>
#include <parlib/dtls.h>
// XXX: what do we do to make include the right libraries here?

bool opal_debug_threads = false;

static void opal_thread_construct(opal_thread_t *t);

static lithe_context_t * opal_main_thread;

struct opal_tsd_key_value {
    opal_tsd_key_t key;
    opal_tsd_destructor_t destructor;
};

static struct opal_tsd_key_value *opal_tsd_key_values = NULL;
static int opal_tsd_key_values_count = 0;

OBJ_CLASS_INSTANCE(opal_thread_t,
                   opal_object_t,
                   opal_thread_construct, NULL);

static size_t __context_stack_size = 1<<20;
static struct wfl sched_zombie_list = WFL_INITIALIZER(sched_zombie_list);
static struct wfl context_zombie_list = WFL_INITIALIZER(context_zombie_list);

static void context_block(lithe_sched_t *__this, lithe_context_t *__context);
static void context_exit(lithe_sched_t *__this, lithe_context_t *__context);


static const lithe_sched_funcs_t lithe_sched_funcs = {
  .hart_request        = lithe_fork_join_sched_hart_request,
  .hart_enter          = lithe_fork_join_sched_hart_enter,
  .hart_return         = lithe_fork_join_sched_hart_return,
  .sched_enter         = lithe_fork_join_sched_sched_enter,
  .sched_exit          = lithe_fork_join_sched_sched_exit,
  .child_enter         = lithe_fork_join_sched_child_enter,
  .child_exit          = lithe_fork_join_sched_child_exit,
  .context_block       = context_block,
  .context_unblock     = lithe_fork_join_sched_context_unblock,
  .context_yield       = lithe_fork_join_sched_context_yield,
  .context_exit        = context_exit
};

static void __ctx_free(lithe_context_t *ctx)
{
  printf("%d:%s\n",getpid(), __FUNCTION__);
  /*
	if (wfl_size(&context_zombie_list) < 1000) {
		wfl_insert(&context_zombie_list, ctx);
	} else {
		int stacksize = ctx->stack.size
      + ctx->stack_offset;
		int ret = munmap(ctx->stack.bottom, stacksize);
		assert(!ret);
	}
  */
}

void __ctx_setup(lithe_fork_join_context_t *ctx, size_t stacksize)
{
/*
 * The libgomp implementation uses a list here to reuse dead contexts, I'll implement that if need be.
 */
	void *stackbot = mmap(
       		0, stacksize, PROT_READ|PROT_WRITE|PROT_EXEC,
               		MAP_PRIVATE|MAP_ANONYMOUS, -1, 0
                       );
	/* hard code the cache line size for now, will do better for production */
	int offset = roundup(sizeof(lithe_fork_join_context_t), 0x40);

    printf("%d:%s\n",getpid(),__FUNCTION__);
	if (stackbot == MAP_FAILED)
		abort();
	ctx->stack_offset = offset;
	ctx->context.stack.bottom = stackbot;
	ctx->context.stack.size = stacksize - offset;
}

lithe_fork_join_sched_t *lithe_sched_alloc()
{
  /* Allocate all the scheduler data together. */

  struct sched_data {
    lithe_fork_join_sched_t sched;
    lithe_fork_join_context_t main_context;
    struct lithe_fork_join_vc_mgmt vc_mgmt[];
  };

  /* Use a zombie list to reuse old schedulers if available, otherwise, create
   * a new one. */
 struct sched_data *s = wfl_remove(&sched_zombie_list);

    printf("%d:%s\n",getpid(),__FUNCTION__);
 
  if (!s) {
    s = parlib_aligned_alloc(PGSIZE,
                             sizeof(*s) + sizeof(struct lithe_fork_join_vc_mgmt) * max_vcores());
    __ctx_setup(&s->main_context, 8192);
    s->sched.vc_mgmt = &s->vc_mgmt[0];
    s->sched.sched.funcs = &lithe_sched_funcs;
  }

  s->main_context.context.data = (void*)false;
  lithe_fork_join_sched_init(&s->sched, &s->main_context);
  
  return &s->sched;
}

/*
 * Constructor
 */
static void opal_thread_construct(opal_thread_t *t)
{
    printf("%d:%s\n",getpid(),__FUNCTION__);
    t->t_run = 0;
    memset(&t->t_handle, 0, sizeof(t->t_handle));
    //t->t_handle = (lithe_handle_t*) NULL; //malloc(sizeof(lithe_handle_t));
}

int opal_thread_start(opal_thread_t *t)
{
    int rc;
    

    printf("%d:%s\n",getpid(),__FUNCTION__);
    t->t_handle.sched = *lithe_sched_alloc();
    if (OPAL_ENABLE_DEBUG) {
	// is there a sentinal value for a bad scheduler?
        if (NULL == t->t_run ) {
            return OPAL_ERR_BAD_PARAM;
        }
    }
    // XXX where is schedule defined? Can this fail? we also need to enable a context.
    lithe_sched_enter((lithe_sched_t*)&t->t_handle.sched);
    __ctx_setup(&t->t_handle.ctxt, 8192);

    lithe_fork_join_context_init(&t->t_handle.sched, &t->t_handle.ctxt,(void (*)(void *))t->t_run, t);
    return OPAL_SUCCESS;
}


int opal_thread_join(opal_thread_t *t, void **thr_return)
{
    // so what does thread look like here?
    printf("%d:%s\n",getpid(),__FUNCTION__);
    lithe_fork_join_sched_join_one(&t->t_handle.sched);
    
    return OPAL_SUCCESS; //(rc == 0) ? OPAL_SUCCESS : OPAL_ERROR;
}


bool opal_thread_self_compare(opal_thread_t *t)
{
    printf("%d:%s\n",getpid(),__FUNCTION__);
    return &t->t_handle.ctxt.context == lithe_context_self();
}


opal_thread_t *opal_thread_get_self(void)
{
    printf("%d:%s\n",getpid(),__FUNCTION__);
    opal_thread_t *t = OBJ_NEW(opal_thread_t);
    t->t_handle.ctxt.context = *lithe_context_self();
    return t;
}

void opal_thread_kill(opal_thread_t *t, int sig)
{
  printf("%d:%s\n",getpid(),__FUNCTION__);
  // you're actually killing someone that is not you.
  // since that is the case you need to switch to that context and delete it.
  // but that is interesting, because that's a signal.
  lithe_fork_join_context_destroy(&t->t_handle.ctxt);
  //pthread_kill(t->t_handle, sig);
}

int opal_tsd_key_create(opal_tsd_key_t *key,
                    opal_tsd_destructor_t destructor)
{
    int rc;

    printf("%d:%s: making key\n",getpid(), __FUNCTION__);
    *key = (opal_tsd_key_t)dtls_key_create(destructor);
    //printf("%s: made key %p\n",__FUNCTION__, key);
    if ((lithe_context_self() == opal_main_thread)) {
        opal_tsd_key_values = (struct opal_tsd_key_value *)realloc(opal_tsd_key_values, (opal_tsd_key_values_count+1) * sizeof(struct opal_tsd_key_value));
        opal_tsd_key_values[opal_tsd_key_values_count].key = *key;
        opal_tsd_key_values[opal_tsd_key_values_count].destructor = destructor;
        opal_tsd_key_values_count ++;
    }
    return OPAL_SUCCESS;
    return rc;
}

int opal_tsd_keys_destruct()
{
   printf("%d:%s\n",getpid(),__FUNCTION__);
/*
    int i;
    void * ptr;
    for (i=0; i<opal_tsd_key_values_count; i++) {
        if(OPAL_SUCCESS == opal_tsd_getspecific(opal_tsd_key_values[i].key, &ptr)) {
                opal_tsd_key_values[i].destructor(ptr);
                opal_tsd_setspecific(opal_tsd_key_values[i].key, NULL);
            }
        }
    }
    if (0 < opal_tsd_key_values_count) {
        free(opal_tsd_key_values);
        opal_tsd_key_values_count = 0;
    }
*/
    return OPAL_SUCCESS;
}

void opal_thread_set_main() {
  printf("%d:%s\n",getpid(),__FUNCTION__);
  opal_main_thread = lithe_context_self();
}


static void context_block(lithe_sched_t *sched, lithe_context_t *context)
{
  uint64_t completed;
  printf("%d:%s\n",getpid(),__FUNCTION__);
  lithe_fork_join_sched_context_block(sched, context);
  completed = (uint64_t)context->data;
  if(completed)
    lithe_fork_join_sched_join_one((lithe_fork_join_sched_t*)sched);
}

static void context_exit(lithe_sched_t *__this, lithe_context_t *context)
{
  printf("%d:%s\n",getpid(),__FUNCTION__);
  lithe_hart_request(-1);
  // so what do I do with this?
  //lithe_fork_join_context_cleanup((lithe_fork_join_context_t*)context);
  __ctx_free(context);
  //libgomp_lithe_sched_decref((libgomp_lithe_sched_t*)__this);
}
