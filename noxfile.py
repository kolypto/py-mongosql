import nox.sessions


nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = ['tests']


@nox.session(python=['3.7', '3.8'])
def tests(session: nox.sessions.Session):
    """ Run all tests """
    session.install('poetry')
    session.run('poetry', 'install')

    # Test
    session.run('pytest', 'tests/', '--cov=myproject')
