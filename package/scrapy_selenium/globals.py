from werkzeug.local import LocalProxy, LocalStack

_global_ctx_stack = LocalStack()
_global_ctx_err_msg = 'Working outside of scrapy context.'

def _find_g():
    top = _global_ctx_stack.top
    if top is None:
        raise RuntimeError(_global_ctx_err_msg)
    return top

g = LocalProxy(_find_g)
