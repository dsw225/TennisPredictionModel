from math import pow, sqrt, log, exp, fabs, pi
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

START_RATING = 1500
RATING_SCALE = 173.7178  # Scaling factor to convert Glicko-2 rating to traditional rating scale
RD_START = 350
TAU = 0.5  # System constant
RATING_VOL = .06

# Constants
RATING_PERIOD = 40

class Rating:
    def __init__(self, rating=START_RATING, rd=RD_START, vol=RATING_VOL):
        self.rating = (rating - START_RATING) / RATING_SCALE
        self.rd = rd / RATING_SCALE
        self.vol = vol
        self.last_match_date = datetime(1900, 1, 1)

    def getRating(self):
        return (self.rating * 173.7178) + 1500 

    def setRating(self, rating):
        self.rating = (rating - 1500) / 173.7178

    def getRd(self):
        return self.rd * 173.7178

    def setRd(self, rd):
        self.rd = rd / 173.7178

    def getVol(self):
        return self.vol
    
    def get_pre_rating_rd(self, current_date=None):
        current_date = current_date
        time_difference = (current_date - self.last_match_date).days

        # Factor to increase RD over time, can be adjusted based on requirements.
        time_factor = (time_difference / RATING_PERIOD) + 1
        return min(sqrt(self.rd**2 + ((self.vol ** 2)*time_factor)), RD_START) * 173.7178

    def pre_rating_rd(self, current_date=None):
        current_date = current_date
        time_difference = (current_date - self.last_match_date).days

        # Factor to increase RD over time, can be adjusted based on requirements.
        time_factor = (time_difference / RATING_PERIOD) + 1
        self.rd = min(sqrt(self.rd**2 + ((self.vol ** 2)*time_factor)), RD_START)
        self.last_match_date = current_date

    def update_player(self, rating_list, rd_list, outcome_list, current_date=None):

        rating_list = [(x - START_RATING) / RATING_SCALE for x in rating_list]
        rd_list = [x / RATING_SCALE for x in rd_list]

        v = self._v(rating_list, rd_list)
        self.vol = self._newVol(rating_list, rd_list, outcome_list, v)
        self._preRatingRD() # If commented out it's so I can call outside
        
        self.rd = 1 / sqrt((1 / pow(self.rd, 2)) + (1 / v))
        
        tempSum = 0
        for i in range(len(rating_list)):
            tempSum += self._g(rd_list[i]) * \
                       (outcome_list[i] - self._e(rating_list[i], rd_list[i]))
        self.rating += pow(self.rd, 2) * tempSum


        # Update last match date
        self.last_match_date = current_date

    def _newVol(self, rating_list, rd_list, outcome_list, v):
        a = log(self.vol ** 2)
        eps = 0.000001
        A = a
        delta = self._delta(rating_list, rd_list, outcome_list, v)
        if delta ** 2 > (self.rd ** 2 + v):
            B = log(delta ** 2 - self.rd ** 2 - v)
        else:
            k = 1
            while self._f(a - k * sqrt(TAU ** 2), delta, v, a) < 0:
                k += 1
            B = a - k * sqrt(TAU ** 2)

        fA = self._f(A, delta, v, a)
        fB = self._f(B, delta, v, a)

        while fabs(B - A) > eps:
            C = A + (A - B) * fA / (fB - fA)
            fC = self._f(C, delta, v, a)
            if fC * fB < 0:
                A = B
                fA = fB
            else:
                fA /= 2
            B = C
            fB = fC

        return exp(A / 2)

    def _f(self, x, delta, v, a):
        ex = exp(x)
        num1 = ex * (delta ** 2 - self.rating ** 2 - v - ex)
        denom1 = 2 * (self.rating ** 2 + v + ex) ** 2
        return num1 / denom1 - (x - a) / TAU ** 2

    def _delta(self, rating_list, rd_list, outcome_list, v):
        temp_sum = 0
        for i in range(len(rating_list)):
            temp_sum += self._g(rd_list[i]) * (outcome_list[i] - self._e(rating_list[i], rd_list[i]))
        return v * temp_sum

    def _v(self, rating_list, rd_list):
        temp_sum = 0
        for i in range(len(rating_list)):
            temp_e = self._e(rating_list[i], rd_list[i])
            temp_sum += self._g(rd_list[i]) ** 2 * temp_e * (1 - temp_e)
        return 1 / temp_sum

    def _e(self, p2rating, p2rd):
        return 1 / (1 + exp(-self._g(p2rd) * (self.rating - p2rating)))

    def _g(self, rd):
        return 1 / sqrt(1 + 3 * rd ** 2 / pi ** 2)