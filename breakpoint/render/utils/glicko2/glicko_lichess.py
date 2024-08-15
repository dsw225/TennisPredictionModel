from math import log, sqrt, exp, pi
from datetime import datetime

class RatingCalculator:

    DEFAULT_RATING = 1500.0
    DEFAULT_DEVIATION = 350.0
    DEFAULT_VOLATILITY = 0.06
    DEFAULT_TAU = 0.75
    MULTIPLIER = 173.7178
    CONVERGENCE_TOLERANCE = 0.000001
    ITERATION_MAX = 1000
    DAYS_PER_MILLI = 1.0 / (1000 * 60 * 60 * 24)

    def __init__(self, init_volatility=DEFAULT_VOLATILITY, tau=DEFAULT_TAU, rating_periods_per_day=None):
        self.tau = tau
        self.default_volatility = init_volatility
        if rating_periods_per_day:
            self.rating_periods_per_milli = rating_periods_per_day * self.DAYS_PER_MILLI
        else:
            self.rating_periods_per_milli = 0

    def update_ratings(self, results, skip_deviation_increase=False):
        for player in results.get_participants():
            elapsed_rating_periods = 0 if skip_deviation_increase else 1
            if results.get_results(player):
                self.calculate_new_rating(player, results.get_results(player), elapsed_rating_periods)
            else:
                player.set_working_rating(player.get_glicko2_rating())
                player.set_working_rating_deviation(
                    self.calculate_new_rd(player.get_glicko2_rating_deviation(), player.get_volatility(), elapsed_rating_periods)
                )
                player.set_working_volatility(player.get_volatility())

        for player in results.get_participants():
            player.finalise_rating()

        results.clear()

    def preview_deviation(self, player, rating_period_end_date, reverse=False):
        elapsed_rating_periods = 0
        if self.rating_periods_per_milli != 0 and player.get_last_rating_period_end_date():
            interval = (rating_period_end_date - player.get_last_rating_period_end_date()).total_seconds() * 1000
            elapsed_rating_periods = interval * self.rating_periods_per_milli
        if reverse:
            elapsed_rating_periods = -elapsed_rating_periods

        new_rd = self.calculate_new_rd(player.get_glicko2_rating_deviation(), player.get_volatility(), elapsed_rating_periods)
        return self.convert_rating_deviation_to_original_glicko_scale(new_rd)

    def calculate_new_rating(self, player, results, elapsed_rating_periods):
        phi = player.get_glicko2_rating_deviation()
        sigma = player.get_volatility()
        a = log(sigma ** 2)
        delta = self.delta(player, results)
        v = self.v(player, results)

        A = a
        B = log(delta ** 2 - phi ** 2 - v) if delta ** 2 > phi ** 2 + v else a - 1 * abs(self.tau)

        while self.f(B, delta, phi, v, a, self.tau) < 0:
            B = a - 1 * abs(self.tau)

        fA = self.f(A, delta, phi, v, a, self.tau)
        fB = self.f(B, delta, phi, v, a, self.tau)

        iterations = 0
        while abs(B - A) > self.CONVERGENCE_TOLERANCE and iterations < self.ITERATION_MAX:
            iterations += 1
            C = A + (A - B) * fA / (fB - fA)
            fC = self.f(C, delta, phi, v, a, self.tau)

            if fC * fB <= 0:
                A = B
                fA = fB
            else:
                fA /= 2.0

            B = C
            fB = fC

        if iterations == self.ITERATION_MAX:
            raise RuntimeError(f"Convergence fail at {iterations} iterations")

        new_sigma = exp(A / 2.0)
        player.set_working_volatility(new_sigma)

        phi_star = self.calculate_new_rd(phi, new_sigma, elapsed_rating_periods)
        new_phi = 1.0 / sqrt((1.0 / phi_star ** 2) + (1.0 / v))

        player.set_working_rating(
            player.get_glicko2_rating() + (new_phi ** 2) * self.outcome_based_rating(player, results)
        )
        player.set_working_rating_deviation(new_phi)
        player.increment_number_of_results(len(results))

    def f(self, x, delta, phi, v, a, tau):
        return (
            exp(x) * (delta ** 2 - phi ** 2 - v - exp(x)) /
            (2.0 * (phi ** 2 + v + exp(x)) ** 2)
        ) - ((x - a) / tau ** 2)

    def g(self, deviation):
        return 1.0 / sqrt(1.0 + (3.0 * deviation ** 2 / pi ** 2))

    def e(self, player_rating, opponent_rating, opponent_deviation):
        return 1.0 / (1.0 + exp(-1.0 * self.g(opponent_deviation) * (player_rating - opponent_rating)))

    def v(self, player, results):
        v = 0.0
        for result in results:
            opponent = result.get_opponent(player)
            v += (
                self.g(opponent.get_glicko2_rating_deviation()) ** 2 *
                self.e(player.get_glicko2_rating(), opponent.get_glicko2_rating(), opponent.get_glicko2_rating_deviation()) *
                (1.0 - self.e(player.get_glicko2_rating(), opponent.get_glicko2_rating(), opponent.get_glicko2_rating_deviation()))
            )
        return 1 / v

    def delta(self, player, results):
        return self.v(player, results) * self.outcome_based_rating(player, results)

    def outcome_based_rating(self, player, results):
        outcome_based_rating = 0.0
        for result in results:
            opponent = result.get_opponent(player)
            outcome_based_rating += (
                self.g(opponent.get_glicko2_rating_deviation()) *
                (result.get_score(player) - self.e(
                    player.get_glicko2_rating(),
                    opponent.get_glicko2_rating(),
                    opponent.get_glicko2_rating_deviation()
                ))
            )
        return outcome_based_rating

    def calculate_new_rd(self, phi, sigma, elapsed_rating_periods):
        return sqrt(phi ** 2 + elapsed_rating_periods * sigma ** 2)

    @staticmethod
    def convert_rating_to_original_glicko_scale(rating):
        return rating * RatingCalculator.MULTIPLIER + RatingCalculator.DEFAULT_RATING

    @staticmethod
    def convert_rating_to_glicko2_scale(rating):
        return (rating - RatingCalculator.DEFAULT_RATING) / RatingCalculator.MULTIPLIER

    @staticmethod
    def convert_rating_deviation_to_original_glicko_scale(rating_deviation):
        return rating_deviation * RatingCalculator.MULTIPLIER

    @staticmethod
    def convert_rating_deviation_to_glicko2_scale(rating_deviation):
        return rating_deviation / RatingCalculator.MULTIPLIER

    def get_default_rating(self):
        return self.DEFAULT_RATING

    def get_default_volatility(self):
        return self.default_volatility

    def get_default_rating_deviation(self):
        return self.DEFAULT_DEVIATION
