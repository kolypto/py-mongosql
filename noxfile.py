import nox.sessions

PYTHON_VERSIONS = ['3.7', '3.8', '3.9']
SQLALCHEMY_VERIONS = [
    *(f'1.2.{x}' for x in range(0, 1 + 19) if x not in (9,)),
    *(f'1.3.{x}' for x in range(0, 1 + 23)),
]

nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = [
    'tests',
    # 'tests_sqlalchemy',
]


@nox.session(python=PYTHON_VERSIONS)
def tests(session: nox.sessions.Session, sqlalchemy=None):
    """ Run all tests """
    session.install('poetry')
    session.run('poetry', 'install')
    
    # Specific package versions
    if sqlalchemy:
        session.install(f'sqlalchemy=={sqlalchemy}')

    # Test
    session.run('pytest', 'tests/', '--cov=mongosql')


@nox.session(python=PYTHON_VERSIONS[-1])
@nox.parametrize('sqlalchemy', SQLALCHEMY_VERIONS)
def tests_sqlalchemy(session: nox.sessions.Session, sqlalchemy):
    """ Test against a specific SqlAlchemy version """
    tests(session, sqlalchemy)
