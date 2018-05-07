class Router():
    """
    All the routing should go here
    """
    def __init__(self):
        self.routes = [
                        # healthcheck
                        ('^/healthcheck', 'index'),
                        # mysite related routes
                        ('^/helloworld/(?P<test_name>.+)', 'hello_world'),
                       ]

    def __call__(self, *args, **kwargs):
        return self.routes