class PathRegistry:
    _instance = None
    _paths = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PathRegistry, cls).__new__(cls)
            cls._paths = {}
        return cls._instance

    def set_path(self, alias, path):
        self._paths[alias] = path

    def get_path(self, alias):
        return self._paths.get(alias)

    def all_paths(self):
        return dict(self._paths)