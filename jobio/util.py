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


def validate_dict_types(input_dict, required_fields=None, verbose=False):
    if not required_fields:
        required_fields = {}
    for var, _type in required_fields.items():
        if not present_in(var, input_dict, verbose=verbose):
            return False
        if not isinstance(input_dict[var], _type):
            if verbose:
                print(
                    "variable: {} value: {} is of incorrect type: {}".format(
                        var, input_dict[var], type(input_dict[var])
                    )
                )
            return False
    return True


def validate_dict_values(input_dict, required_values=None, verbose=False):
    if not required_values:
        required_values = {}

    for var, required_value in required_values.items():
        if not present_in(var, input_dict, verbose=verbose):
            return False
        if required_value and not input_dict[var]:
            if verbose:
                print(
                    "The required variable: {} was not set in: {}".format(
                        var, input_dict
                    )
                )
            return False
    return True


def save_results(path, results):
    save_dir = os.path.dirname(path)
    if not os.path.exists(save_dir):
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
