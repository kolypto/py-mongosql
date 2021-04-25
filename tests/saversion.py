from distutils.version import LooseVersion

from mongosql.sa_version import SA_VERSION, SA_12, SA_13, SA_14


def SA_VERSION_IN(min_version, max_version):
    """ Check that SqlAlchemy version lies within a range

    This is slow; only use in unit-tests!
    """
    return LooseVersion(min_version) <= LooseVersion(SA_VERSION) <= LooseVersion(max_version)


def SA_SINCE(version):
    """ Check SqlAlchemy >= version """
    return LooseVersion(SA_VERSION) >= LooseVersion(version)


def SA_UNTIL(version):
    """ Check SqlAlchemy <= version """
    return LooseVersion(SA_VERSION) <= LooseVersion(version)
