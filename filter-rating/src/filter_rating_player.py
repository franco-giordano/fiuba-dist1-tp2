class FilterRatingPlayer:
    def should_pass(self, player_dict):
        pass_q2 = True
        if player_dict['winner'] and player_dict['rating'] <= 1000:
            pass_q2 = False

        pass_q4 = player_dict['rating'] > 2000
        
        return pass_q2, pass_q4
