from math import pow, sqrt, log, exp, fabs, pi
import math
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

START_RATING = 1500
RATING_SCALE = 173.7178  # Scaling factor to convert Glicko-2 rating to traditional rating scale
RD_START = 350
TAU = 0.5  # System constant
RATING_VOL = .06
EPSILON =0.000001
RATING_PERIOD = 45 #Days
START_DATE = datetime(1800, 1, 1).date()

class Rating:
    # The system constant, which constrains
    # the change in volatility over time.

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
     
    def __init__(self, rating = START_RATING, rd = RD_START, vol = RATING_VOL):
        # For testing purposes, preload the values
        # assigned to an unrated player.
        self.setRating(rating)
        self.setRd(rd)
        self.vol = vol
        self.last_match_date = START_DATE

    def get_pre_rating_rd(self, current_date=None):
        if self.last_match_date != START_DATE and self.last_match_date != START_DATE != None:
            time_difference = (current_date - self.last_match_date).days
        else:
           time_difference = 1

        # Factor to increase RD over time, can be adjusted based on requirements.
        time_factor = (time_difference / RATING_PERIOD) + 1
        return sqrt(self.__rd ** 2 + self.vol ** 2 * time_factor) * RATING_SCALE
            
    def _preRatingRD(self, current_date=None):
        """ Calculates and updates the player's rating deviation for the
        beginning of a rating period.
        
        preRatingRD() -> None
        
        """
        if self.last_match_date != START_DATE and self.last_match_date != START_DATE != None:
            time_difference = (current_date - self.last_match_date).days
        else:
           time_difference = 1

        # Factor to increase RD over time, can be adjusted based on requirements.
        time_factor = (time_difference / RATING_PERIOD) + 1
        self.__rd = sqrt(self.__rd ** 2 + self.vol ** 2 * time_factor)
        self.last_match_date = current_date
        
    def update_player(self, rating_list, RD_list, outcome_list, current_date=None):
        """ Calculates the new rating and rating deviation of the player.
        
        update_player(list[int], list[int], list[bool]) -> None
        
        """
        # Convert the rating and rating deviation values for internal use.
        rating_list = [(x - START_RATING) / RATING_SCALE for x in rating_list]
        RD_list = [x / RATING_SCALE for x in RD_list]

        v = self._v(rating_list, RD_list)
        self.vol = self._newVol(rating_list, RD_list, outcome_list, v)
        self._preRatingRD(current_date)
        
        self.__rd = 1 / math.sqrt((1 / math.pow(self.__rd, 2)) + (1 / v))
        
        tempSum = 0
        for i in range(len(rating_list)):
            tempSum += self._g(RD_list[i]) * \
                       (outcome_list[i] - self._E(rating_list[i], RD_list[i]))
        self.__rating += math.pow(self.__rd, 2) * tempSum

    #step 5        
    def _newVol(self, rating_list, RD_list, outcome_list, v):
        """ Calculating the new volatility as per the Glicko2 system. 
        
        Updated for Feb 22, 2012 revision. -Leo
        
        _newVol(list, list, list, float) -> float
        
        """
        #step 1
        a = math.log(self.vol**2)
        eps = EPSILON
        A = a
        
        #step 2
        B = None
        delta = self._delta(rating_list, RD_list, outcome_list, v)
        tau = TAU
        if (delta ** 2)  > ((self.__rd**2) + v):
          B = math.log(delta**2 - self.__rd**2 - v)
        else:        
          k = 1
          while self._f(a - k * math.sqrt(tau**2), delta, v, a) < 0:
            k = k + 1
          B = a - k * math.sqrt(tau **2)
        
        #step 3
        fA = self._f(A, delta, v, a)
        fB = self._f(B, delta, v, a)
        
        #step 4
        while math.fabs(B - A) > eps:
          #a
          C = A + ((A - B) * fA)/(fB - fA)
          fC = self._f(C, delta, v, a)
          #b
          if fC * fB <= 0:
            A = B
            fA = fB
          else:
            fA = fA/2.0
          #c
          B = C
          fB = fC
        
        #step 5
        return math.exp(A / 2)
        
    def _f(self, x, delta, v, a):
      ex = math.exp(x)
      num1 = ex * (delta**2 - self.__rating**2 - v - ex)
      denom1 = 2 * ((self.__rating**2 + v + ex)**2)
      return  (num1 / denom1) - ((x - a) / (TAU**2))
        
    def _delta(self, rating_list, RD_list, outcome_list, v):
        """ The delta function of the Glicko2 system.
        
        _delta(list, list, list) -> float
        
        """
        tempSum = 0
        for i in range(len(rating_list)):
            tempSum += self._g(RD_list[i]) * (outcome_list[i] - self._E(rating_list[i], RD_list[i]))
        return v * tempSum
        
    def _v(self, rating_list, RD_list):
        """ The v function of the Glicko2 system.
        
        _v(list[int], list[int]) -> float
        
        """
        tempSum = 0
        for i in range(len(rating_list)):
            tempE = self._E(rating_list[i], RD_list[i])
            tempSum += math.pow(self._g(RD_list[i]), 2) * tempE * (1 - tempE)
        return 1 / tempSum
        
    def _E(self, p2rating, p2RD):
        """ The Glicko E function.
        
        _E(int) -> float
        
        """
        return 1 / (1 + math.exp(-1 * self._g(p2RD) * \
                                 (self.__rating - p2rating)))
        
    def _g(self, RD):
        """ The Glicko2 g(RD) function.
        
        _g() -> float
        
        """
        return 1 / math.sqrt(1 + 3 * math.pow(RD, 2) / math.pow(math.pi, 2))