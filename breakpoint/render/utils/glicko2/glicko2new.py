# -*- coding: utf-8 -*-
"""
    glicko2
    ~~~~~~~

    The Glicko2 rating system.

    :copyright: (c) 2012 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
import math
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

RATING = 1500
RD = 350
VOL = 0.06
TAU = 1.0
EPSILON = 0.000001
RATING_PERIOD = 45
START_DATE = datetime(1800, 1, 1).date()


class Rating(object):
    def __init__(self, rating=RATING, rd=RD, vol=VOL, last_date=START_DATE):
        self.rating = rating
        self.rd = rd
        self.vol = vol
        self.last_date = last_date

    def __repr__(self):
        c = type(self)
        args = (c.__module__, c.__name__, self.rating, self.rd, self.vol)
        return '%s.%s(rating=%.3f, rd=%.3f, vol=%.3f)' % args


class Glicko2(object):
    def __init__(self, rating=RATING, rd=RD, vol=VOL, last_date=START_DATE, tau=TAU, epsilon=EPSILON):
        self.rating = rating
        self.rd = rd
        self.vol = vol
        self.last_date = last_date
        self.tau = tau
        self.epsilon = epsilon

    def create_rating(self, rating=None, rd=None, vol=None, last_date=None):
        if rating is None:
            rating = self.rating
        if rd is None:
            rd = self.rd
        if vol is None:
            vol = self.vol
        if last_date is None:
            last_date = self.last_date
        return Rating(rating, rd, vol, last_date)

    def scale_down(self, rating, ratio=173.7178):
        rating = (rating.rating - self.rating) / ratio
        rd = rating.rd / ratio
        return self.create_rating(rating, rd, rating.vol)

    def scale_up(self, rating, ratio=173.7178):
        rating = rating.rating * ratio + self.rating
        rd = rating.rd * ratio
        return self.create_rating(rating, rd, rating.vol)

    def reduce_impact(self, rating):
        """The original form is `g(RD)`. This function reduces the impact of
        games as a function of an opponent's RD.
        """
        return 1. / math.sqrt(1 + (3 * rating.rd ** 2) / (math.pi ** 2))

    def expect_score(self, rating, other_rating, impact):
        return 1. / (1 + math.exp(-impact * (rating.rating - other_rating.rating)))

    def determine_vol(self, rating, difference, variance):
        """Determines new vol."""
        rd = rating.rd
        difference_squared = difference ** 2
        # 1. Let a = ln(s^2), and define f(x)
        alpha = math.log(rating.vol ** 2)

        def f(x):
            """This function is twice the conditional log-posterior density of
            rd, and is the optimality criterion.
            """
            tmp = rd ** 2 + variance + math.exp(x)
            a = math.exp(x) * (difference_squared - tmp) / (2 * tmp ** 2)
            b = (x - alpha) / (self.tau ** 2)
            return a - b

        # 2. Set the initial values of the iterative algorithm.
        a = alpha
        if difference_squared > rd ** 2 + variance:
            b = math.log(difference_squared - rd ** 2 - variance)
        else:
            k = 1
            while f(alpha - k * math.sqrt(self.tau ** 2)) < 0:
                k += 1
            b = alpha - k * math.sqrt(self.tau ** 2)
        # 3. Let fA = f(A) and f(B) = f(B)
        f_a, f_b = f(a), f(b)
        # 4. While |B-A| > e, carry out the following steps.
        # (a) Let C = A + (A - B)fA / (fB-fA), and let fC = f(C).
        # (b) If fCfB < 0, then set A <- B and fA <- fB; otherwise, just set
        #     fA <- fA/2.
        # (c) Set B <- C and fB <- fC.
        # (d) Stop if |B-A| <= e. Repeat the above three steps otherwise.
        while abs(b - a) > self.epsilon:
            c = a + (a - b) * f_a / (f_b - f_a)
            f_c = f(c)
            if f_c * f_b < 0:
                a, f_a = b, f_b
            else:
                f_a /= 2
            b, f_b = c, f_c
        # 5. Once |B-A| <= e, set s' <- e^(A/2)
        return math.exp(1) ** (a / 2)

    def rate(self, rating, series):
        # Step 2. For each player, convert the rating and RD's onto the
        #         Glicko-2 scale.
        rating = self.scale_down(rating)
        # Step 3. Compute the quantity v. This is the estimated variance of the
        #         team's/player's rating based only on game outcomes.
        # Step 4. Compute the quantity difference, the estimated improvement in
        #         rating by comparing the pre-period rating to the performance
        #         rating based only on game outcomes.
        variance_inv = 0
        difference = 0
        if not series:
            # If the team didn't play in the series, do only Step 6
            rd_star = math.sqrt(rating.rd ** 2 + rating.vol ** 2)
            return self.scale_up(self.create_rating(rating.rating, rd_star, rating.vol))
        for actual_score, other_rating in series:
            other_rating = self.scale_down(other_rating)
            impact = self.reduce_impact(other_rating)
            expected_score = self.expect_score(rating, other_rating, impact)
            variance_inv += impact ** 2 * expected_score * (1 - expected_score)
            difference += impact * (actual_score - expected_score)
        difference /= variance_inv
        variance = 1. / variance_inv
        # Step 5. Determine the new value, vol', ot the vol. This
        #         computation requires iteration.
        vol = self.determine_vol(rating, difference, variance)
        # Step 6. Update the rating deviation to the new pre-rating period
        #         value, rd*.
        rd_star = math.sqrt(rating.rd ** 2 + vol ** 2)
        # Step 7. Update the rating and RD to the new values, rating' and rd'.
        rd = 1. / math.sqrt(1 / rd_star ** 2 + 1 / variance)
        rating = rating.rating + rd ** 2 * (difference / variance)
        # Step 8. Convert ratings and RD's back to original scale.
        return self.scale_up(self.create_rating(rating, rd, vol))

    def rate_1vs1(self, rating1, rating2, drawn=False):
        return (self.rate(rating1, [(DRAW if drawn else WIN, rating2)]),
                self.rate(rating2, [(DRAW if drawn else LOSS, rating1)]))

    def quality_1vs1(self, rating1, rating2):
        expected_score1 = self.expect_score(rating1, rating2, self.reduce_impact(rating1))
        expected_score2 = self.expect_score(rating2, rating1, self.reduce_impact(rating2))
        expected_score = (expected_score1 + expected_score2) / 2
        return 2 * (0.5 - abs(0.5 - expected_score))