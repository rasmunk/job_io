import os
import json
import shutil


def create_dir(path):
    try:
        os.makedirs(path)
        return True
    except IOError as err:
        print("Failed to create: {}, err: {}".format(path, err))
    return False


def remove_dir(path):
    try:
        shutil.rmtree(path)
        return True
    except OSError as err:
        print("Failed to remove: {}, err: {}".format(path, err))
    return False


def present_in(var, collection, verbose=False):
    if var not in collection:
        if verbose:
            print("variable: {} not present in: {}".format(var, collection))
        return False
    return True


def validate_dict_types(input_dict, required_fields=None, verbose=False, throw=False):
    if not required_fields:
        required_fields = {}
    for var, _type in required_fields.items():
        if not present_in(var, input_dict, verbose=verbose):
            if throw:
                raise KeyError(
                    "Missing required: {} in {}".format(var, required_fields)
                )
            return False
        if not isinstance(input_dict[var], _type):
            if verbose:
                print(
                    "variable: {} value: {} is of incorrect type: {}".format(
                        var, input_dict[var], type(input_dict[var])
                    )
                )
            if throw:
                raise TypeError("{}: should be {}".format(var, _type))
            return False
    return True


def validate_dict_values(input_dict, required_values=None, verbose=False, throw=False):
    if not required_values:
        required_values = {}

    for var, required_value in required_values.items():
        if not present_in(var, input_dict, verbose=verbose):
            if throw:
                raise KeyError("Missing required: {} in {} ".format(var, input_dict))
            return False
        if required_value and not input_dict[var]:
            if verbose:
                print(
                    "The required variable: {} was not set in: {}".format(
                        var, input_dict
                    )
                )
            if throw:
                raise ValueError(
                    "{} doesn't have a value: {}".format(var, required_value)
                )
            return False
    return True


def save_results(path, results):
    save_dir = os.path.dirname(path)
    if save_dir and not os.path.exists(save_dir):
        if not create_dir(save_dir):
            return False
    try:
        with open(path, "w") as fh:
            try:
                json.dump(results, fh)
            except TypeError as j_err:
                print("Failed to serialize to json: {}".format(j_err))
                return False
        return True
    except IOError as err:
        print("Failed to save results: {}".format(err))
    return False


def load_kubernetes_secrets(directory, secret_keys, strip_file_newline=True):
    loaded_secrets = {}
    for secret_key in secret_keys:
        value_path = os.path.join(directory, secret_key)
        if os.path.exists(value_path):
            if os.path.islink(value_path):
                value_path = os.path.realpath(value_path)
            if os.path.isfile(value_path) and not os.path.islink(value_path):
                content = None
                try:
                    with open(value_path, "rb") as fh:
                        content = fh.read()
                except IOError as err:
                    print("Failed to read file: {}".format(err))
                decoded = content.decode("utf-8")
                if strip_file_newline:
                    decoded = decoded.replace("\n", "")
                loaded_secrets[secret_key] = decoded
    return loaded_secrets
