
def raise_auto_arg_type_error(argument_name: str | list[str] | tuple[str, ...] | set[str] | None = None):
    """
    Raise a TypeError with a standard message for arguments not matching the parameter's requested type.
    """
    message = f"TypeError with one or more of argument(s): {argument_name}"
    try:
        import inspect
        import types
        current_frame = inspect.currentframe()
        current_function_name = None
        caller_frame = None
        if isinstance(current_frame, types.FrameType):
            current_function_name = inspect.getframeinfo(current_frame).function
            # caller_frame = inspect.stack()[1].frame
            caller_frame = current_frame.f_back
        if isinstance(caller_frame, types.FrameType):
            caller_function_name = inspect.getframeinfo(caller_frame).function
            message = f"TypeError in function '{caller_function_name}'."
            signature = inspect.signature(caller_frame.f_globals[caller_function_name])
            message += f"\nSignature: {caller_function_name}{signature}"
            arg_names = []
            if argument_name is None:
                arg_names = list(signature.parameters.keys())
            elif isinstance(argument_name, (list, tuple, set)):
                arg_names = list(argument_name)
            elif isinstance(argument_name, str):
                arg_names = [argument_name]
            else:
                current_function_name_msg_str = f"in '{current_function_name}'" if current_function_name else ""
                received_argument_name_type_str = type(argument_name).__name__
                raise TypeError(f"Invalid argument name type {current_function_name_msg_str} for argument 'argument_name': {received_argument_name_type_str}")
            for arg_name in arg_names:
                received_arg_type = type(caller_frame.f_locals[arg_name])
                received_arg_type_str = received_arg_type.__name__ if isinstance(received_arg_type, type) else received_arg_type
                received_arg = caller_frame.f_locals[arg_name]
                expected_arg_type = signature.parameters[arg_name].annotation
                expected_arg_type_str = expected_arg_type.__name__ if isinstance(expected_arg_type, type) else expected_arg_type
                if received_arg_type != expected_arg_type:
                    message += f"\nArgument '{arg_name}' must be of type '{expected_arg_type_str}', but {caller_function_name} got {received_arg_type_str}: {received_arg}"
    except Exception as e:
        raise TypeError(message + f"\nAdditional error while generating error message:\n{e}")
        # raise Exception(f"Failed to generate type error message: {e}")
    raise TypeError(message)
