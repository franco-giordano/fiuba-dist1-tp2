class FilterLadder:
    def __init__(self, route_1v1, route_team):
        self.route_1v1 = route_1v1
        self.route_team = route_team

    def select_route(self, match_dict):
        if match_dict['ladder'] == 'RM_1v1':
            return self.route_1v1
        elif match_dict['ladder'] == 'RM_TEAM':
            return self.route_team
        else:
            return None
