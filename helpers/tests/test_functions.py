import pandas
import unittest
import os
from helpers.functions import agg_inv, country_to_region

class TestFunctions(unittest.TestCase):
    inv_for_agg = pandas.DataFrame.from_dict([
        {
            "Target_Region": "CHN",
            "CB": "1",
            "year": 2013,
            "investment_value": 10
        },
        {
            "Target_Region": "CHN",
            "CB": "2",
            "year": 2013,
            "investment_value": 20
        },
        {
            "Target_Region": "CHN",
            "CB": "2",
            "year": 2014,
            "investment_value": 20
        },
        {
            "Target_Region": "CHN",
            "CB": "2",
            "year": 2014,
            "investment_value": 30
        },
        {
            "Target_Region": "USA",
            "CB": "3",
            "year": 2015,
            "investment_value": 30
        },
        {
            "Target_Region": "USA",
            "CB": "4",
            "year": 2015,
            "investment_value": 40
        }
    ])

    def test_agg_inv_mean(self):
        agg = agg_inv("mean", self.inv_for_agg, ["Target_Region", "CB"], ["year"], ["investment_value"])
        self.assertEqual(agg.to_dict(), {('investment_value', 2013): {('CHN', '1'): 10, ('CHN', '2'): 20, ('USA', '3'): 0, ('USA', '4'): 0},
                                                          ('investment_value', 2014): {('CHN', '1'): 0, ('CHN', '2'): 25, ('USA', '3'): 0, ('USA', '4'): 0},
                                                          ('investment_value', 2015): {('CHN', '1'): 0, ('CHN', '2'): 0, ('USA', '3'): 30, ('USA', '4'): 40}})


    def test_agg_inv_count(self):
        agg = agg_inv("count", self.inv_for_agg, ["Target_Region", "CB"], ["year"], ["investment_value"])
        self.assertEqual(agg.to_dict(), {('investment_value', 2013): {('CHN', '1'): 1, ('CHN', '2'): 1, ('USA', '3'): 0, ('USA', '4'): 0},
                                         ('investment_value', 2014): {('CHN', '1'): 0, ('CHN', '2'): 2, ('USA', '3'): 0, ('USA', '4'): 0},
                                         ('investment_value', 2015): {('CHN', '1'): 0, ('CHN', '2'): 0, ('USA', '3'): 1, ('USA', '4'): 1}})

    def test_agg_inv_sum(self):
        agg = agg_inv("sum", self.inv_for_agg, ["Target_Region", "CB"], ["year"], ["investment_value"])
        self.assertEqual(agg.to_dict(), {('investment_value', 2013): {('CHN', '1'): 10, ('CHN', '2'): 20, ('USA', '3'): 0, ('USA', '4'): 0},
                                                          ('investment_value', 2014): {('CHN', '1'): 0, ('CHN', '2'): 50, ('USA', '3'): 0, ('USA', '4'): 0},
                                                          ('investment_value', 2015): {('CHN', '1'): 0, ('CHN', '2'): 0, ('USA', '3'): 30, ('USA', '4'): 40}})

    def test_country_to_region_null(self):
        input_data = pandas.DataFrame.from_dict([{
            "investor_country": None,
            "target_country": None
        }])
        output_data = country_to_region(input_data)
        self.assertEqual(output_data["Target_Region"].iloc[0], "Unknown")
        self.assertEqual(output_data["investor_Region"].iloc[0], "Unknown")

        input_data = pandas.DataFrame.from_dict([{
            "investor_country": "FRA",
            "target_country": None
        }])
        output_data = country_to_region(input_data)
        self.assertEqual(output_data["Target_Region"].iloc[0], "Unknown")
        self.assertEqual(output_data["investor_Region"].iloc[0], "ROW")

        input_data = pandas.DataFrame.from_dict([{
            "investor_country": None,
            "target_country": "FRA"
        }])
        output_data = country_to_region(input_data)
        self.assertEqual(output_data["Target_Region"].iloc[0], "ROW")
        self.assertEqual(output_data["investor_Region"].iloc[0], "Unknown")

    def test_country_to_region_fra(self):
        input_data = pandas.DataFrame.from_dict([{
            "investor_country": "FRA",
            "target_country": "FRA"
        }])
        output_data = country_to_region(input_data)
        self.assertEqual(output_data["Target_Region"].iloc[0], "ROW")
        self.assertEqual(output_data["investor_Region"].iloc[0], "ROW")

    def test_country_to_region_usa(self):
        input_data = pandas.DataFrame.from_dict([{
            "investor_country": "USA",
            "target_country": "USA"
        }])
        output_data = country_to_region(input_data)
        self.assertEqual(output_data["Target_Region"].iloc[0], "USA")
        self.assertEqual(output_data["investor_Region"].iloc[0], "USA")

    def test_country_to_region_cn(self):
        input_data = pandas.DataFrame.from_dict([{
            "investor_country": "CHN",
            "target_country": "HKG"
        }])
        output_data = country_to_region(input_data)
        self.assertEqual(output_data["Target_Region"].iloc[0], "China")
        self.assertEqual(output_data["investor_Region"].iloc[0], "China")

        # try the ordering as well
        input_data = pandas.DataFrame.from_dict([{
            "investor_country": "HKG",
            "target_country": "CHN"
        }])
        output_data = country_to_region(input_data)
        self.assertEqual(output_data["Target_Region"].iloc[0], "China")
        self.assertEqual(output_data["investor_Region"].iloc[0], "China")

