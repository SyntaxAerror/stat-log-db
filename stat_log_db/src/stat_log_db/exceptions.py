
def raise_type_error_with_signature():
    """Generate a standard type error message."""
    # TODO: f"arg must be of type 'argtype', but {inspect.stack()[0][3]} got {type(arg).__name__}"
    message = ""
    try:
        import inspect
        caller_frame = inspect.stack()[1].frame
        import types
        if isinstance(caller_frame, types.FrameType):
            function_name = inspect.getframeinfo(caller_frame).function
            signature = inspect.signature(caller_frame.f_globals[function_name])
            message = f"TypeError in function '{function_name}'. Signature:\n{function_name}{signature}"
    except Exception as e:
        raise Exception(f"Failed to generate type error message: {e}")
    raise TypeError(message)
