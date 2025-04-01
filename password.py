import bcrypt
import hashlib

def sha256_hash(password):
    data = password.encode()
    hasher = hashlib.sha256()
    hasher.update(data)

    return hasher.hexdigest()


def sha256_compare(password, hashed):
    data = sha256_hash(password)

    return data == hashed


def bcrypt_hash(password):
    salt = bcrypt.gensalt(rounds=12)

    password = str.encode(password)

    xpass = bcrypt.hashpw(password, salt)

    new = xpass.decode()

    return new


def bcrypt_compare(password, saved):
    x = str.encode(saved)

    password = str.encode(password)

    same = bcrypt.checkpw(password, x)

    return same


import re


def check_password_strength(password):
    """
    Checks the strength of a password.

    Args:
        password (str): The password to check.

    Returns:
        int: A score indicating the strength of the password.
    """

    # Define the regular expressions for different password strength levels.
    weak_regex = re.compile(r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d).{8,}$")
    medium_regex = re.compile(r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*#?&]).{10,}$")
    strong_regex = re.compile(
        r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*#?&])(?=.*[~!@#$%^&*()_+=-]).{12,}$"
    )

    # Check the password strength.

    if strong_regex.match(password):
        return 2

    elif medium_regex.match(password):
        return 1

    elif weak_regex.match(password):
        return 0

    else:
        return -1
