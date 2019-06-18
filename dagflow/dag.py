# __author__ = 'godq'


class Dag:
    def __init__(self, metadata_dict):
        self.metadata = metadata_dict

    def next_step(self, current_step):
        return None

