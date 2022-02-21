#!/usr/bin/env python3

"""
A utility to help users to install/uninstall the pymongo mask.

Pymongo mask is a fake pymongo which has to be installed in a python
sys.path with higher priority than the real pymongo. It will use the
real pymongo as the backend to be fully compatible with the real
pymongo but it also enable pymongowatch loggers by default.
"""

import argparse
import os
import subprocess
import sys


def is_pymongo_mask_already_installed():
    """
    Determine whether pymongo mask is already installed or not, by
    trying to import the pymongo and check if the imported result is
    the fake module or the real one.

    In the rare case that pymongo mask is installed by the real
    pymongo is not installed this mehtod may mistakenly returns False.
    """
    sys.modules.pop("pymongo", None)
    try:
        pymongo = __import__("pymongo")
    except ImportError:
        return False

    return hasattr(pymongo, "real_pymongo")


def get_pymongo_mask_installed_path():
    """
    Returns the install path of pymongo mask or None if it is not
    found.
    """
    for path in sys.path:
        try:
            link_path = os.path.join(path, "pymongo.py")
            if os.path.islink(link_path):
                return link_path
        except OSError:
            pass

    return None


def get_pymongo_path():
    """
    Returns the importable pymongo path directory.
    """
    sys.modules.pop("pymongo", None)
    try:
        pymongo = __import__("pymongo")
    except ImportError:
        return ""

    return os.path.dirname(os.path.dirname(pymongo.__file__))


def get_available_install_paths():
    """
    Returns the higher priority sys.paths than the one in which
    pymongo is already installed as a list.
    """
    pymongo_path = get_pymongo_path()
    current_dir = os.path.dirname(__file__)

    paths = []
    for path in sys.path:
        try:
            if not path or os.path.samefile(path, current_dir):
                continue
            elif os.path.samefile(path, pymongo_path):
                break
            else:
                paths.append(path)
        except OSError:
            paths.append(path)
    else:
        return []

    return paths


def get_pymongo_mask_source_path():
    """
    Returns the source path of the pymongo mask
    """
    return os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", "pymongo_mask", "pymongo.py"))


def uninstall():
    """
    Uninstalls the pymongo mask
    """
    link_path = get_pymongo_mask_installed_path()
    if not link_path:
        sys.stderr.write("The pymongo mask in not installed.\n")
        return True

    sys.stderr.write(f"Removing the link file in {link_path}.\n")
    try:
        os.unlink(link_path)
    except OSError as exp:
        sys.stderr.write(f"Error while removing the link: {exp}\n")
        return False

    return True


def install(install_dir):
    """
    Installs the pymongo mask in the given `install_dir` directory
    """
    src = get_pymongo_mask_source_path()
    if not os.path.exists(src):
        sys.stderr.write(f"The pymongo mask is not available at {src}.\n")
        return False

    if not os.path.exists(install_dir):
        sys.stderr.write(f"The directory {install_dir} doesn't exist. "
                         f"Creating it...\n")
        try:
            os.makedirs(install_dir)
        except OSError as exp:
            sys.stderr.write(f"Error while creating the directory: {exp}\n")
            return False

    dst = os.path.join(install_dir, "pymongo.py")

    if os.path.exists(dst):
        if os.path.samefile(src, dst):
            sys.stderr.write("The correct symlink is already installed.\n")
            return True
        else:
            sys.stderr.write(
                f"A link with a different path is already installed at "
                f"{dst}. Please first uninstall that link.\n")
            return False

    try:
        sys.stderr.write(f"Creating a symlink: {src} -> {dst}\n")
        os.symlink(src, dst)
    except OSError as exp:
        sys.stderr.write(f"Error while creating the symlink link: {exp}\n")
        return False

    return True


def run_self_with_sudo(*args):
    """
    Run the current python module with the specified arguments with
    "sudo".
    """
    try:
        retcode = subprocess.call(["sudo", sys.executable, __file__] +
                                  list(args))
    except subprocess.SubprocessError:
        retcode = 1

    return retcode


def menu_options():
    """
    Returns a text string and a dictionary (as a tuple) as the
    available interactive options for pymongo mask installer by
    checking whether pymongo mask is already installed or not and
    provide the available options for install/uninstall.

    The text is the human readable options and the dictionary has the
    options (numbers) as keys and a function as the value for each
    option which can be called without any arguments to execute the
    option.
    """
    text = []
    options = {}

    mask_installed_path = get_pymongo_mask_installed_path()
    if is_pymongo_mask_already_installed() or mask_installed_path:
        if mask_installed_path:
            text += [f"pymongo mask is already installed at "
                     f"{mask_installed_path}\n"]
        else:
            text += ["pymongo mask seems installed and importable but no link "
                     "to pymongo.py found in python sys.path. Maybe there is "
                     "an incompatible installation on the system.\n"]
            return "".join(text), options

        text += ["\nYour options:\n"]

        mask_installed_dir = os.path.dirname(mask_installed_path)
        if os.access(mask_installed_dir, os.W_OK):
            text += [f"1) Uninstall from {mask_installed_path}\n"]
            options[1] = lambda: 0 if uninstall() else 1
        else:
            text += [
                f"1) [requires sudo] Uninstall from {mask_installed_path}\n"]
            options[1] = lambda: run_self_with_sudo("--uninstall")

        return "".join(text), options

    text += ["You can choose an option to install a symlink to pymongo.py "
             "mask in a python sys.path with higher priority than the real "
             "pymongo package. The pymongo.py mask module will use the real "
             "pymongo as the backend to exactly replicate the pymongo but it "
             "also enable pymongowatch by default.\n\nYour options:\n"]

    for i, install_path in enumerate(get_available_install_paths()):
        opt = i + 1
        if os.access(install_path, os.W_OK):
            text += [f"{opt}) Install at {install_path}\n"]
            options[opt] = ((lambda path: lambda: 0 if install(path) else 1)
                            (install_path))
        else:
            text += [f"{opt}) [requires sudo] Install at {install_path}\n"]
            options[opt] = (
                (lambda path: lambda: run_self_with_sudo("--install", path))
                (install_path))

    return "".join(text), options


def main():
    """
    The main entrypoint for the application
    """
    parser = argparse.ArgumentParser(
        description="Install/uninstall a fake pymongo module (named pymongo "
        "mask) in a python sys.path directory with higher priority than the "
        "real pymongo. The pymongo mask will use the real pymongo as the "
        "backend to be fully compatible with the real pymongo but it also "
        "enable pymongowatch loggers by default. Without any arguments, this "
        "program will run interactively.")
    parser.add_argument("--available-paths", action="store_true",
                        help="Print available python paths for installation "
                        "and exit.")
    parser.add_argument("-u", "--uninstall", action="store_true",
                        help="Uninstall the pymongo mask and exit.")
    parser.add_argument("-i", "--install", metavar="<dir>",
                        help="Install a symlink to pymongo mask in the <dir> "
                        "and exit.")

    args = parser.parse_args()

    if args.available_paths:
        print("\n".join(get_available_install_paths()))
        exit(0)

    if args.uninstall:
        exit(0 if uninstall() else 1)

    if args.install:
        exit(0 if install(args.install) else 1)

    text, options = menu_options()

    print(text)
    if options:
        opt = input(
            f"Enter an option [{min(options.keys())}"
            f"{'-%d' % max(options.keys()) if len(options) > 1 else ''}] "
            f"or any other value to exit: ")
        try:
            opt = int(opt)
        except ValueError:
            pass
        if opt in options:
            exit(options[opt]())


if __name__ == "__main__":
    main()
