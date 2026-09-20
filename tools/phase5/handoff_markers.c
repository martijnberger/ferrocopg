/* Diagnostic-only boundaries. PyDLL calls these without releasing the GIL. */
__attribute__((noinline, visibility("default")))
void phase5_trace_start(void) { __asm__ volatile("" ::: "memory"); }

__attribute__((noinline, visibility("default")))
void phase5_trace_stop(void) { __asm__ volatile("" ::: "memory"); }
