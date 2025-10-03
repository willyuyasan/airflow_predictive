class appEnvironmentParms:

    def __init__(self, **kwargs):

        app = kwargs.get('app', None)

        if not app:
            self.app = 'trading'

        force_closure_dts_dict = {}
        force_closure_dts_dict['sp500_15m_v3'] = ['2025-10-02 19:45:00']

        self.force_closure_dts_dict = force_closure_dts_dict


ENV_VARS_LS = [
    'GOOGLEAPI_GD_CREDENTIAL'
]