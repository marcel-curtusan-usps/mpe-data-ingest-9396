import os
import logging

# Helper function for loading environment variables
def get_env_variable(key):
    value = os.getenv(key)
    if value is None:
        logging.error(f"Environment variable '{key}' not found.")
        raise ValueError(f"Environment variable '{key}' not found.")
    return value
    
# Set proxy, if needed
https_proxy = get_env_variable('https_proxy')
def set_proxy(enable):
    if https_proxy != "":
        if enable:
            os.environ["HTTPS_PROXY"] = https_proxy
            os.environ["https_proxy"] = https_proxy
            os.environ["HTTP_PROXY"] = https_proxy
            return True, https_proxy
        else:
            os.environ.pop("HTTPS_PROXY", None)
            os.environ.pop("https_proxy", None)
            os.environ.pop("HTTP_PROXY", None)
            return False, ""
    return False, ""