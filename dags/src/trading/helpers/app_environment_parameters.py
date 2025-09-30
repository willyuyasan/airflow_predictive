class appEnvironmentParms:

    def __init__(self, **kwargs):

        app = kwargs.get('app', None)

        if not app:
            self.app = 'trading'



ENV_VARS_LS = [
    'GOOGLEAPI_GD_CREDENTIAL'
]