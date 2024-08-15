from math import pow, sqrt, log, exp, fabs, pi
import math
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

START_RATING = 1500
RATING_SCALE = 173.7178  # Scaling factor to convert Glicko-2 rating to traditional rating scale
RD_START = 350
TAU = .75  # System constant
RATING_VOL = 0.06
EPSILON = 0.000001
RATING_PERIOD = 40  # Days
START_DATE = datetime(1800, 1, 1).date()

class Rating:
    def getRating(self):
        return (self.__rating * RATING_SCALE) + START_RATING

    def setRating(self, rating):
        self.__rating = (rating - START_RATING) / RATING_SCALE

    rating = property(getRating, setRating)

    def getRd(self):
        return self.__rd * RATING_SCALE

    def setRd(self, rd):
        self.__rd = rd / RATING_SCALE

    rd = property(getRd, setRd)

    def getVol(self):
        return self.vol

    def __init__(self, rating=START_RATING, rd=RD_START, vol=RATING_VOL):
        self.setRating(rating)
        self.setRd(rd)
        self.vol = vol
        self.last_match_date = START_DATE

    def get_pre_rating_rd(self, current_date=None):
        if self.last_match_date != START_DATE and current_date is not None:
            time_difference = (current_date - self.last_match_date).days
        else:
            time_difference = 1

        time_factor = (time_difference / RATING_PERIOD) + 1
        return sqrt(self.__rd ** 2 + self.vol ** 2 * time_factor) * RATING_SCALE

    def _preRatingRD(self, current_date=None):
        if self.last_match_date != START_DATE and current_date is not None:
            time_difference = (current_date - self.last_match_date).days
        else:
            time_difference = 1

        time_factor = (time_difference / RATING_PERIOD) + 1
        self.__rd = sqrt(self.__rd ** 2 + self.vol ** 2 * time_factor)
        self.last_match_date = current_date

    def update_player(self, opponent_rating, opponent_rd, outcome, current_date=None):
        opponent_rating = (opponent_rating - START_RATING) / RATING_SCALE
        opponent_rd = opponent_rd / RATING_SCALE

        v = self._v(opponent_rating, opponent_rd)
        self.vol = self._newVol(opponent_rating, opponent_rd, outcome, v)
        self._preRatingRD(current_date)

        self.__rd = 1 / math.sqrt((1 / math.pow(self.__rd, 2)) + (1 / v))

        tempSum = self._g(opponent_rd) * (outcome - self._E(opponent_rating, opponent_rd))
        self.__rating += math.pow(self.__rd, 2) * tempSum

    def _newVol(self, opponent_rating, opponent_rd, outcome, v):
        a = math.log(self.vol**2)
        eps = EPSILON
        A = a

        delta = self._delta(opponent_rating, opponent_rd, outcome, v)
        tau = TAU
        if (delta ** 2) > ((self.__rd**2) + v):
            B = math.log(delta**2 - self.__rd**2 - v)
        else:
            k = 1
            while self._f(a - k * math.sqrt(tau**2), delta, v, a) < 0:
                k += 1
            B = a - k * math.sqrt(tau**2)

        fA = self._f(A, delta, v, a)
        fB = self._f(B, delta, v, a)

        while fabs(B - A) > eps:
            C = A + ((A - B) * fA) / (fB - fA)
            fC = self._f(C, delta, v, a)

            if fC * fB <= 0:
                A = B
                fA = fB
            else:
                fA /= 2.0

            B = C
            fB = fC

        return exp(A / 2)

    def _f(self, x, delta, v, a):
        ex = exp(x)
        num1 = ex * (delta**2 - self.__rating**2 - v - ex)
        denom1 = 2 * ((self.__rating**2 + v + ex)**2)
        return (num1 / denom1) - ((x - a) / (TAU**2))

    def _delta(self, opponent_rating, opponent_rd, outcome, v):
        tempSum = self._g(opponent_rd) * (outcome - self._E(opponent_rating, opponent_rd))
        return v * tempSum

    def _v(self, opponent_rating, opponent_rd):
        tempE = self._E(opponent_rating, opponent_rd)
        return 1 / (pow(self._g(opponent_rd), 2) * tempE * (1 - tempE))

    def _E(self, p2rating, p2RD):
        return 1 / (1 + exp(-1 * self._g(p2RD) * (self.__rating - p2rating)))

    def _g(self, RD):
        return 1 / sqrt(1 + 3 * pow(RD, 2) / pow(pi, 2))
