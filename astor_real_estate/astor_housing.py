import logging
logging.basicConfig(format='%(asctime)s %(funcName)s %(message)s', filename='/var/log/astor_square/astor_housing.log',level=logging.DEBUG)
from astor_schemas import *
import math
from astor_square_utils import *

class UnitTaxInfo(object):
    def __init__(self, bbl=None, connection_pool=None):
        self.connection_pool = connection_pool
        self.query = None

        self.bbl = bbl
        self.neighborhood = None
        self.building_class = None
        self.borough_block_lot = None
        self.address = None
        self.year_built = None
        self.total_units = None
        self.gross_square_feet = None
        self.estimated_gross_income = None
        self.gross_income_per_square_foot = None
        self.estimated_expense = None
        self.expense_per_square_foot = None
        self.net_operating_income = None
        self.net_operating_income_per_square_foot = None
        self.full_market_value = None
        self.market_value_per_square_foot = None
        self.net_present_value = None
        self.net_present_value_per_square_foot = None
        self.last_year_annual_tax = None
        self.this_year_annual_tax = None

        self.full_addr = None

    @property
    def full_address(self):
        if self.full_addr is None and self.address is not None:
            borough = self.bbl[0]
            city = get_borough_city(borough)
            state = 'NY'
            zip = None #getzipcode(self.address, city, state)
            if zip is None:
                zip = ''
            self.full_addr = self.address + ' ' + city + ', ' + state + ' ' + zip
        return self.full_addr.strip()


class Comparable(UnitTaxInfo):

    def __init__(self, bbl=None, connection_pool=None):
        UnitTaxInfo.__init__(self, bbl, connection_pool)
        self.query = 'select DISTINCT * from tax_analysis_city_comparables where borough_block_lot = %s'

        self.bbl = None
        self.neighborhood = None
        self.building_class = None
        self.borough_block_lot = None
        self.address = None
        self.year_built = None
        self.total_units = None
        self.gross_square_feet = None
        self.estimated_gross_income = None
        self.gross_income_per_square_foot = None
        self.estimated_expense = None
        self.expense_per_square_foot = None
        self.net_operating_income = None
        self.full_market_value = None
        self.market_value_per_square_foot = None
        self.comparablebbl = None
        self.annual_tax = None
        self.comp_quality = None
        self.year = None
        self.fiscal_year = None
        self.lat = None
        self.long = None

    def __repr__(self):
        return "<Comparable(bbl={self.bbl!r},comparablebbl={self.comparablebbl!r})>".format(self=self)

    def create_comparable_from_row(self, row):

        self.neighborhood = row[0]
        self.building_class = row[1]
        self.borough_block_lot = row[2]
        self.bbl = self.borough_block_lot.replace('-','') if self.bbl is None else self.bbl
        logging.debug('bbl set to ' + self.bbl + ' from ' + self.borough_block_lot)
        self.address = row[3]
        self.year_built = row[4]
        self.total_units = row[5]
        self.gross_square_feet = row[6]
        self.estimated_gross_income = row[7]
        self.gross_income_per_square_foot = row[8]
        self.estimated_expense = row[9]
        self.expense_per_square_foot = row[10]
        self.net_operating_income = row[11]
        if self.net_operating_income is not None and self.gross_square_feet is not None:
            self.net_operating_income_per_square_foot = self.net_operating_income / self.gross_square_feet
        self.full_market_value = row[12]
        self.market_value_per_square_foot = row[13]
        self.distance_from_subject_in_miles = row[14]
        self.comparablebbl = row[15]
        self.year = row[16]
        self.fiscal_year = row[17]
        self.comp_quality = row[18]
        self.lat = row[19]
        self.long = row[20]

        return

    def load_comparable_attributes(self):
        if self.bbl is None:
            return

        query_bbl = create_dashed_bbl(self.bbl)
        dbconnection = self.connection_pool.getconn()
        cursor = dbconnection.cursor()
        cursor.execute(self.query, (query_bbl,))
        row = cursor.fetchone()

        self.neighborhood = row[0]
        self.building_class = row[1]
        self.borough_block_lot = row[2]
        self.bbl = self.borough_block_lot.replace('-','')
        self.address = row[3]
        self.year_built = row[4]
        self.total_units = row[5]
        self.gross_square_feet = row[6]
        self.estimated_gross_income = row[7]
        self.gross_income_per_square_foot = row[8]
        self.estimated_expense = row[9]
        self.expense_per_square_foot = row[10]
        self.net_operating_income = row[11]
        if self.net_operating_income is not None and self.gross_square_feet is not None:
            self.net_operating_income_per_square_foot = self.net_operating_income / self.gross_square_feet
        self.full_market_value = row[12]
        self.market_value_per_square_foot = row[13]
        self.comparablebbl = row[14]


    def get_json(self):
        if self.bbl is None and self.connection_pool is not None:
            logging.debug('loading comparable attributes')
            self.load_comparable_attributes()
        elif self.bbl is None and self.connection_pool is None:
            logging.debug('No bbl. Returning blank result')
            return '{}'
        schema = ComparableSchema()
        return schema.dump(self)


class PropertyTaxAnalysis(UnitTaxInfo):

    def __init__(self, bbl=None, connection_pool=None):
        UnitTaxInfo.__init__(self, bbl, connection_pool)

        self.query = '''SELECT * 
                        FROM building_tax_analysis b
                        LEFT JOIN bbl_locations l ON
                        b.borough_block_lot = l.borough_block_lot 
                        WHERE b.borough_block_lot = %s AND fiscal_year IS NOT NULL ORDER BY fiscal_year DESC'''

        self.bbl = bbl
        self.last_year_total_market_value = None
        self.this_year_total_market_value = None
        self.last_year_assessed_value = None
        self.this_year_assessed_value = None
        self.last_year_transitional_assessed_value = None
        self.this_year_transitional_assessed_value = None
        self.lat = None
        self.long = None

    def __repr__(self):
        return "<PropertyTaxAnalysis(bbl={self.bbl!r})>".format(self=self)

    def load_tax_analysis_attributes(self):
        if self.bbl is None:
            return

        query_bbl = create_dashed_bbl(self.bbl)

        dbconnection = self.connection_pool.getconn()
        cursor = dbconnection.cursor()
        cursor.execute(self.query, (query_bbl,))
        row = cursor.fetchone()

        self.neighborhood = row[0]
        self.building_class = row[1]
        self.borough_block_lot = row[2]
        self.address = row[3]
        self.year_built = row[4]
        self.total_units = row[5]
        self.gross_square_feet = row[6]
        self.estimated_gross_income = row[7]
        self.gross_income_per_square_foot = row[8]
        self.estimated_expense = row[9]
        self.expense_per_square_foot = row[10]
        self.net_operating_income = row[11]
        if self.net_operating_income is not None and self.gross_square_feet is not None:
            self.net_operating_income_per_square_foot = self.net_operating_income / self.gross_square_feet
        self.full_market_value = row[12]
        self.market_value_per_square_foot = row[13]
        self.last_year_total_market_value = row[14]
        self.this_year_total_market_value = row[15]
        self.last_year_assessed_value = row[16]
        self.this_year_assessed_value = row[17]
        self.last_year_transitional_assessed_value = row[18]
        self.this_year_transitional_assessed_value = row[19]
        self.last_year_annual_tax = row[20]
        self.this_year_annual_tax = row[21]
        self.lat = row[27]
        self.long = row[28]

        self.connection_pool.putconn(dbconnection)
        return

    def get_json(self):
        if self.neighborhood is None and self.connection_pool is not None:
            self.load_tax_analysis_attributes()
        elif self.neighborhood is None and self.connection_pool is None:
            return ''
        try:
            schema = PropertyTaxAnalysisSchema()
            result = schema.dump(self)
        except Exception as e:
            logging.error('problem getting schema: ' + str(e))
            result = json.dump({})
        return schema.dump(self)


class CondoTaxAnalysis(PropertyTaxAnalysis):
    def __init__(self, bbl=None, connection_pool=None):
        PropertyTaxAnalysis.__init__(self, bbl, connection_pool)

        self.query = 'select * from condo_tax_analysis where borough_block_lot = %s'

    def __repr__(self):
        return "<CondoTaxAnalysis(bbl={self.bbl!r})>".format(self=self)

class UnitAndBuildingTaxAnalysis(object):

    def __init__(self, unit_tax_analysis, building_tax_analysis):
        self.unit_tax_analysis = unit_tax_analysis
        if self.unit_tax_analysis.neighborhood is None and self.unit_tax_analysis.connection_pool is not None:
            self.unit_tax_analysis.load_tax_analysis_attributes()
        self.building_tax_analysis = building_tax_analysis
        if self.building_tax_analysis.neighborhood is None and self.building_tax_analysis.connection_pool is not None:
            self.building_tax_analysis.load_tax_analysis_attributes()

    def __repr__(self):
        return "<UnitAndBuildingTaxAnalysis(unit_tax_analysis={self.unit_tax_analysis!r}, building_tax_analysis={self.building_tax_analysis!r})>".format(self=self)

    def get_json(self):
        schema = UnitAndBuildingTaxAnalysisSchema()
        return schema.dump(self)

class CityComparable(Comparable):
    def __init__(self, bbl=None, connection_pool=None):
        Comparable.__init__(self, bbl, connection_pool)

        self.unadjusted_income_query = '''SELECT 
                                        estimated_gross_income, 
                                        gross_income_per_square_foot, 
                                        estimated_expense, 
                                        expense_per_square_foot, 
                                        net_operating_income, 
                                        full_market_value, 
                                        market_value_per_square_foot 
                                    FROM city_comparables_unadjusted 
                                    WHERE year = %s
                                    AND borough_block_lot = %s'''

        self.unadjusted_estimated_gross_income = None
        self.unadjusted_gross_income_per_square_foot = None
        self.unadjusted_estimated_expense = None
        self.unadjusted_expense_per_square_foot = None
        self.unadjusted_net_operating_income = None
        self.unadjusted_full_market_value = None
        self.unadjusted_market_value_per_square_foot = None

    def add_unadjusted_data_from_row(self, row):
        self.unadjusted_estimated_gross_income = row[0]
        self.unadjusted_gross_income_per_square_foot = row[1]
        self.unadjusted_estimated_expense = row[2]
        self.unadjusted_expense_per_square_foot = row[3]
        self.unadjusted_net_operating_income = row[4]
        self.unadjusted_full_market_value = row[5]
        self.unadjusted_market_value_per_square_foot = row[6]

    def get_json(self):
        if self.bbl is None and self.connection_pool is not None:
            logging.debug('loading comparable attributes')
            self.load_comparable_attributes()
        elif self.bbl is None and self.connection_pool is None:
            logging.debug('No bbl. Returning blank result')
            return '{}'
        schema = CityComparableSchema()
        return schema.dump(self)

class CityComparables(object):

    def __init__(self, bbl=None, connection_pool=None):
        self.query = """SELECT DISTINCT
                            c.neighborhood,
                            c.building_class,
                            c.borough_block_lot,
                            c.address,
                            c.year_built,
                            c.total_units,
                            c.gross_square_feet,
                            c.estimated_gross_income,
                            c.gross_income_per_square_foot,
                            c.estimated_expense,
                            c.expense_per_square_foot,
                            c.net_operating_income,
                            c.full_market_value,
                            c.market_value_per_square_foot,
                            c.distance_from_subject_in_miles,
                            c.comparableof,
                            c.year,
                            c.fiscal_year,
                            s.score,
                            l.lat,
                            l.long 
                            FROM tax_analysis_city_comparables c
                            LEFT JOIN similar_bbls s on REPLACE(c.borough_block_lot, '-', '') = s.similar_bbl
                            AND REPLACE(c.comparableof, '-','') = s.bbl AND s.city_comp = True
                            LEFT JOIN bbl_locations l ON l.borough_block_lot = c.borough_block_lot
                            where c.comparableof = %s"""
        self.comparables = []
        self.comparableof = bbl
        self.connection_pool = connection_pool

        query_bbl = create_dashed_bbl(self.comparableof)

        dbconnection = self.connection_pool.getconn()
        cursor = dbconnection.cursor()
        logging.debug('executing query ' + self.query + ' with argument ' + query_bbl)
        cursor.execute(self.query, (query_bbl,))
        rows = cursor.fetchall()
        logging.debug('got ' + str(len(rows)) + ' comparable results')

        for row in rows:
            comparable = CityComparable()
            comparable.create_comparable_from_row(row)
            cursor.execute(comparable.unadjusted_income_query, (comparable.year, comparable.borough_block_lot))
            unadjusted_row = cursor.fetchone()
            if unadjusted_row is not None:
                comparable.add_unadjusted_data_from_row(unadjusted_row)
            self.comparables.append(comparable)
        self.connection_pool.putconn(dbconnection)
        return

    def get_json(self):
        result = [c.get_json() for c in self.comparables]
        json_result = json.dumps(result)
        return result

class RecommendedComparables(object):

    def __init__(self, bbl=None, connection_pool=None):

        self.comparable_bbls_query = 'SELECT similar_bbl, score FROM similar_bbls WHERE bbl = %s'
        query_template = 'select DISTINCT * from tax_analysis_recommended_comparables where borough_block_lot IN ('
        query_template = '''SELECT DISTINCT
                            c.neighborhood,
                            c.building_class,
                            c.borough_block_lot,
                            c.address,
                            c.year_built,
                            c.total_units,
                            c.gross_square_feet,
                            c.estimated_gross_income,
                            c.gross_income_per_square_foot,
                            c.estimated_expense,
                            c.expense_per_square_foot,
                            c.net_operating_income,
                            c.full_market_value,
                            c.market_value_per_square_foot,
                            c.distance_from_subject_in_miles,
                            c.annual_tax,
                            c.comparableof,
                            c.year,
                            c.fiscal_year,
                            l.lat,
                            l.long
                            FROM tax_analysis_recommended_comparables c
                            LEFT JOIN bbl_locations l ON l.borough_block_lot = c.borough_block_lot
                            where c.borough_block_lot IN (
                            '''
        self.comparables = []
        self.comparableof = bbl
        self.connection_pool = connection_pool

        query_bbl = create_dashed_bbl(self.comparableof)

        dbconnection = self.connection_pool.getconn()
        cursor = dbconnection.cursor()

        logging.debug('executing query ' + self.comparable_bbls_query + ' with argument ' + bbl)
        cursor.execute(self.comparable_bbls_query, (bbl,))
        rows = cursor.fetchall()
        if rows is None or len(rows) == 0:
            return
        recommended_bbls = [create_dashed_bbl(row[0]) for row in rows]

        scores = {}
        for row in rows:
            scores[row[0]] = row[1]

        self.query = query_template + ','.join(['%s']*len(recommended_bbls)) + ')'


        logging.debug('executing query ' + self.query + ' with argument ' + str(recommended_bbls))
        cursor.execute(self.query, tuple(recommended_bbls))
        rows = cursor.fetchall()
        logging.debug('got ' + str(len(rows)) + ' comparable results')
        for row in rows:
            comparable = Comparable()
            self.create_recommended_comparable_from_row(comparable, row)
            if comparable.borough_block_lot.replace('-','') in scores.keys():
                comparable.comp_quality = scores[comparable.borough_block_lot.replace('-','')]
            self.comparables.append(comparable)
        self.connection_pool.putconn(dbconnection)
        return

    def create_recommended_comparable_from_row(self, comparable, row):
        comparable.neighborhood = row[0]
        comparable.building_class = row[1]
        comparable.borough_block_lot = row[2]
        comparable.bbl = comparable.borough_block_lot.replace('-','') if comparable.bbl is None else comparable.bbl
        logging.debug('bbl set to ' + comparable.bbl + ' from ' + comparable.borough_block_lot)
        comparable.address = row[3]
        comparable.year_built = row[4]
        comparable.total_units = row[5]
        comparable.gross_square_feet = row[6]
        comparable.estimated_gross_income = row[7]
        comparable.gross_income_per_square_foot = row[8]
        comparable.estimated_expense = row[9]
        comparable.expense_per_square_foot = row[10]
        comparable.net_operating_income = row[11]
        if comparable.net_operating_income is not None and comparable.gross_square_feet is not None and comparable.gross_square_feet != 0:
            comparable.net_operating_income_per_square_foot = comparable.net_operating_income / comparable.gross_square_feet
            comparable.net_present_value = comparable.net_operating_income/ (.06 - .02)
            comparable.net_present_value_per_square_foot = comparable.net_present_value / comparable.gross_square_feet
        comparable.full_market_value = row[12]
        comparable.market_value_per_square_foot = row[13]
        comparable.distance_from_subject_in_miles = row[14]
        comparable.annual_tax = row[15]
        comparable.comparableof = row[16]
        comparable.year = row[17]
        comparable.fiscal_year = row[18]
        comparable.lat = row[19]
        comparable.long = row[20]

    def get_json(self):
        result = [c.get_json() for c in self.comparables]
        json_result = json.dumps(result)
        return result

'''
neighborhood                          | text                  |           |          | 
 building_class                        | text                  |           |          | 
 borough_block_lot                     | character varying(15) |           |          | 
 address                               | text                  |           |          | 
 year_built                            | integer               |           |          | 
 total_units                           | integer               |           |          | 
 gross_square_feet                     | double precision      |           |          | 
 estimated_gross_income                | double precision      |           |          | 
 gross_income_per_square_foot          | double precision      |           |          | 
 estimated_expense                     | double precision      |           |          | 
 expense_per_square_foot               | double precision      |           |          | 
 net_operating_income                  | double precision      |           |          | 
 full_market_value                     | double precision      |           |          | 
 market_value_per_square_foot          | double precision      |           |          | 
 last_year_total_market_value          | double precision      |           |          | 
 this_year_total_market_value          | double precision      |           |          | 
 last_year_assessed_value              | double precision      |           |          | 
 this_year_assessed_value              | double precision      |           |          | 
 last_year_transitional_assessed_value | double precision      |           |          | 
 this_year_transitional_assessed_value | double precision      
'''

class Building(object):
    def __init__(self, bbl=None, connection_pool = None):
        self.bbl = bbl
        self.connection_pool = connection_pool
        self._init()

    def _init(self):
        self.dbconnection = None
        self.address = None
        self.lotarea = None
        self.bldgarea = None
        self.comarea = None
        self.resarea = None
        self.officearea = None
        self.retailarea = None
        self.garagearea = None
        self.strgearea = None
        self.factryarea = None
        self.otherarea = None
        self.numfloors = None
        self.unitsres = None
        self.unitstotal = None
        self.yearbuilt = None
        self.yearalter1 = None
        self.yearalter2 = None
        self.xcoord = None
        self.ycoord = None
        self.gr_sqft = None
        self.property_tax = None
        self.nearby_buildings = []
        self.sales = []
        return

    def __repr__(self):
        return "<Bulding(bbl={self.bbl!r})>".format(self=self)

    def load_building_attributes(self):
        query = """SELECT bbl, address, zipcode, lotarea, bldgarea, comarea, resarea, officearea, retailarea, garagearea, strgearea, factryarea, otherarea,
                numfloors, unitsres, unitstotal, yearbuilt, yearalter1, yearalter2, xcoord, ycoord FROM pluto_all WHERE bbl = %s"""
        dbconnection = self.connection_pool.getconn()
        cursor = dbconnection.cursor()
        cursor.execute(query, (self.bbl,))
        description = cursor.description
        column_names = [d[0] for d in description]
        column_types = [d[1] for d in description]
        results = cursor.fetchone()
        self.address = results[1] + ' NEW YORK, NY ' + str(results[2])
        self.lotarea = results[3]
        self.bldgarea = results[4]
        self.comarea = results[5]
        self.resarea = results[6]
        self.officearea = results[7]
        self.retailarea = results[8]
        self.garagearea = results[9]
        self.strgearea = results[10]
        self.factryarea = results[11]
        self.otherarea = results[12]
        self.numfloors = results[13]
        self.unitsres = results[14]
        self.unitstotal = results[15]
        self.yearbuilt = results[16]
        self.yearalter1 = results[17]
        self.yearalter2 = results[18]
        self.xcoord = results[19]
        self.ycoord = results[20]

        query = 'SELECT gr_sqft FROM tc234 WHERE bble=%s'
        cursor.execute(query, (self.bbl,))
        row = cursor.fetchone()
        if row is None:
            query = 'SELECT gr_sqft FROM tc1 WHERE bble=%s'
            cursor.execute(query, (self.bbl,))
            row = cursor.fetchone()
        if row is not None:
            self.gr_sqft = row[0]

        tax_query = 'SELECT tax_year, tax_bill FROM tax_records WHERE bbl=%s AND tax_bill IS NOT NULL ORDER BY bill_date DESC;'
        cursor.execute(tax_query, (self.bbl,))
        row = cursor.fetchone()
        if row is not None:
            self.property_tax = row[1]

        self.connection_pool.putconn(dbconnection)

        return

    def get_attributes_as_array(self):
        attribute_array = [ \
        self.lotarea, \
        self.bldgarea, \
        self.comarea, \
        self.resarea, \
        self.officearea, \
        self.retailarea, \
        self.garagearea, \
        self.strgearea, \
        self.factryarea, \
        self.otherarea, \
        self.numfloors, \
        self.unitsres, \
        self.unitstotal, \
        self.yearbuilt, \
        self.yearalter1, \
        self.yearalter2 \
        ]
        return attribute_array

    def get_json(self):
        if self.xcoord is None and self.connection_pool is not None:
            self.load_building_attributes()
        elif self.xcoord is None and self.connection_pool is None:
            return ''
        schema = BuildingSchema()
        return schema.dump(self)

    def _get_location_of_bbl(self, bbl):
        query = '''select xcoord, ycoord FROM pluto_all WHERE bbl = %s'''
        dbconnection = self.connection_pool.getconn()
        cursor = dbconnection.cursor()
        cursor.execute(query, (bbl,))
        result = cursor.fetchone()
        self.connection_pool.putconn(dbconnection)
        return (result[0], result[1])

    def _distance(self, x1, y1, x2, y2):
        return math.sqrt( (x2-x1)*(x2-x1) + (y2-y1)*(y2-y1) )

    def load_nearby_buildings(self, distance=750):
        dbconnection = self.connection_pool.getconn()
        cursor = dbconnection.cursor()
        if (self.xcoord is None):
            coords = self._get_location_of_bbl(self.bbl)
            self.xcoord = coords[0]
            self.ycoord = coords[1]
        x1 = self.xcoord + distance
        x2 = self.xcoord - distance
        y1 = self.ycoord + distance
        y2 = self.ycoord - distance
        borough = int(self.bbl[0])
        borough_string = get_borough_string(borough)
        query = '''select
            borough, block, lot, bbl::text AS bbl, address, zipcode,
            lotarea, bldgarea, comarea, resarea, officearea, retailarea, garagearea, strgearea, factryarea, otherarea,
            numfloors, unitsres, unitstotal, yearbuilt, yearalter1, yearalter2, xcoord, ycoord
            from pluto_all WHERE borough = %s AND 
            xcoord > %s AND xcoord < %s AND ycoord > %s AND ycoord < %s'''
        cursor.execute(query, (borough_string, x2, x1, y2, y1))
        rows = cursor.fetchall()
        for results in rows:
            bbl = results[3]
            if bbl == self.bbl:
                continue
            bldg = Building(bbl)
            bldg.address = results[4] + ' NEW YORK, NY ' + str(results[5])
            bldg.lotarea = results[6]
            bldg.bldgarea = results[7]
            bldg.comarea = results[8]
            bldg.resarea = results[9]
            bldg.officearea = results[10]
            bldg.retailarea = results[11]
            bldg.garagearea = results[12]
            bldg.strgearea = results[13]
            bldg.factryarea = results[14]
            bldg.otherarea = results[15]
            bldg.numfloors = results[16]
            bldg.unitsres = results[17]
            bldg.unitstotal = results[18]
            bldg.yearbuilt = results[19]
            bldg.yearalter1 = results[20]
            bldg.yearalter2 = results[21]
            bldg.xcoord = results[22]
            bldg.ycoord = results[23]
            if self._distance(self.xcoord, self.ycoord, bldg.xcoord, bldg.ycoord) <= 750 \
                    and self._distance(self.xcoord, self.ycoord, bldg.xcoord, bldg.ycoord) != 0:
                self.nearby_buildings.append(bldg)
            query = 'SELECT gr_sqft FROM tc234 WHERE bble=%s'
            cursor.execute(query, (bbl,))
            row = cursor.fetchone()
            if row is None:
                query = 'SELECT gr_sqft FROM tc1 WHERE bble=%s'
                cursor.execute(query, (bbl,))
                row = cursor.fetchone()
            if row is not None:
                bldg.gr_sqft = row[0]
            tax_query = 'SELECT tax_year, tax_bill FROM tax_records WHERE bbl=%s AND tax_bill IS NOT NULL ORDER BY bill_date DESC;'
            cursor.execute(tax_query, (bbl,))
            row = cursor.fetchone()
            if row is not None:
                bldg.property_tax = row[1]

        self.connection_pool.putconn(dbconnection)

        # will be quicker to calculate radius here, anyway

    def get_units_in_building(self):

        dbconnection = self.connection_pool.getconn()
        cursor = dbconnection.cursor()

        borough = self.bbl[0]
        block = int(self.bbl[1:6])
        lot = int(self.bbl[6:10])
        units = []
        unit_bbls = []
        if lot > 7500:
            # this is a condo building
            address_query = '''SELECT hnum_lo, hnum_hi, str_name FROM tc234 WHERE bble=%s'''
            cursor.execute(address_query, (self.bbl,))
            row = cursor.fetchone()
            hnum_lo = row[0]
            hnum_hi = row[1]
            str_name = row[2]
            unit_query = "SELECT bble FROM tc234 WHERE bble LIKE %s AND (hnum_lo=%s OR hnum_hi=%s) AND str_name=%s"
            cursor.execute(unit_query, (self.bbl[0:6]+'%', hnum_lo, hnum_hi, str_name,))
            rows = cursor.fetchall()
            self.connection_pool.putconn(dbconnection)
            unit_bbls = [r[0] for r in rows]
            for unit_bbl in unit_bbls:
                condo_unit = CondoUnit(unit_bbl, self.bbl, self.connection_pool)
                units.append(condo_unit)
        self.units = units
        return units


class ApartmentBuilding(Building):

    def __init__(self, bbl=None, connection_pool=None):
        Building.__init__(self, bbl, connection_pool)
        self._init()

    def _init(self):
        self.cur_fv_l = None
        self.cur_fv_t = None
        self.new_fv_l = None
        self.new_fv_t = None
        self.curavl = None
        self.curavt = None
        self.curexl = None
        self.curext = None
        self.curavl_a = None
        self.curavt_a = None
        self.curexl_a = None
        self.curext_a = None
        self.tn_avt = None
        self.tn_avl = None
        self.tn_ext = None
        self.tn_avl_a = None
        self.tn_avt_a = None
        self.tn_exl_a = None
        self.tn_ext_a = None
        self.fn_avl = None
        self.fn_avt = None
        self.fn_exl = None
        self.fn_avl_a = None
        self.fn_avt_a = None
        self.fn_exl_a = None
        self.fn_ext_a = None

    def load_building_attributes(self):
        Building.load_building_attributes(self)
        query = '''SELECT * FROM tc234 WHERE bble=%s'''
        dbconnection = self.connection_pool.getconn()

        cursor = dbconnection.cursor()
        cursor.execute(query, (self.bbl,))
        row = cursor.fetchone()

        description = cursor.description
        column_names = [d[0] for d in description]
        column_types = [d[1] for d in description]

        for varname in vars(self).keys():
            try:
                idx = column_names.index(varname)
            except ValueError:
                continue
            vars(self)[varname] = row[idx]

    def _load(self):
        if self.connection_pool is None:
            return None
        self.load_building_attributes()
        query = '''SELECT * FROM tc234 WHERE bble=%s'''
        altquery = '''SELECT * FROM tc1 WHERE bble=%s'''
        dbconnection = self.connection_pool.getconn()

        cursor = dbconnection.cursor()
        cursor.execute(query, (self.bbl,))
        row = cursor.fetchone()
        if row is None:
            cursor.execute(altquery, (self.bbl,))
            row = cursor.fetchone()
        if row is None:
            return
        description = cursor.description
        column_names = [d[0] for d in description]
        column_types = [d[1] for d in description]

        for varname in vars(self).keys():
            try:
                idx = column_names.index(varname)
            except ValueError:
                continue
            vars(self)[varname] = row[idx]

    def get_json(self):
        if self.xcoord is None and self.connection_pool is not None:
            self.load_building_attributes()
        elif self.xcoord is None and self.connection_pool is None:
            return ''
        schema = ApartmentBuildingSchema()
        return schema.dump(self)



class CoopBuilding(ApartmentBuilding):
    def __init__(self, bbl=None, connection_pool=None):
        ApartmentBuilding.__init__(self, bbl, connection_pool)

    pass


class CondoBuilding(Building):
    def __init__(self, bbl=None, connection_pool=None):
        Building.__init__(self, bbl, connection_pool)

    def get_json(self):
        if self.xcoord is None and self.connection_pool is not None:
            self.load_building_attributes()
        elif self.xcoord is None and self.connection_pool is None:
            return ''
        schema = CondoBuildingSchema()
        return schema.dump(self)

class Unit(object):
    def __init__(self, id=None, building_bbl=None, connection_pool=None):
        self.id = id
        self.building_bbl = building_bbl
        self.connection_pool = connection_pool
        self._init()

    def _init(self):
        self.gr_sqft = None
        self.aptno = None


class CoopUnit(Unit):
    def __init__(self, bbl=None, building_bbl=None, connection_pool=None):
        self.bbl = bbl
        self.building_bbl = building_bbl
        Unit.__init__(self, bbl, connection_pool)
        self.sales = []


class CondoUnit(Unit):
    def __init__(self, bbl=None, building_bbl=None, connection_pool=None):
        self.bbl = bbl
        super(CondoUnit, self).__init__(bbl, building_bbl, connection_pool)
        self._init()
        self._load()

    def _load(self):
        if self.connection_pool is None:
            return None
        query = '''SELECT * FROM tc234 WHERE bble=%s'''
        dbconnection = self.connection_pool.getconn()

        cursor = dbconnection.cursor()
        cursor.execute(query, (self.bbl,))
        row = cursor.fetchone()

        description = cursor.description
        column_names = [d[0] for d in description]
        column_types = [d[1] for d in description]

        for varname in vars(self).keys():
            try:
                idx = column_names.index(varname)
            except ValueError:
                continue
            vars(self)[varname] = row[idx]

        sales_queries = """SELECT DocumentId, doctype, borough, block, lot, 
                            DocDate, DocAmount, PartyType, PartyName FROM getallsales(%s,%s,%s);"""
        borough = int(self.bbl[0])
        block = str(int(self.bbl[1:6]))
        lot = str(int(self.bbl[6:10]))
        cursor.execute(sales_queries, (borough, block, lot,))
        rows = cursor.fetchall()
        sales = {}
        for row in rows:
            #def __init__(self, price=None, date=None, seller=None, buyer=None):
            docid = row[0]
            if docid not in sales.keys():
                sale = {}
                sale['price'] = row[6]
                sale['date'] = row[5]
                if row[7] == '2':
                    sale['buyer'] = row[8]
                else:
                    sale['seller'] = row[8]
                sales[docid] = sale
            else:
                sale = sales[docid]
                if row[7] == '2':
                    sale['buyer'] = row[8]
                else:
                    sale['seller'] = row[9]
        for docid, sale in sales.iteritems():
            property_sale = PropertySale(sale['price'], sale['date'], sale['seller'], sale['buyer'])
            self.sales.append(property_sale)

        self.sales.sort(key=lambda x: x.date)

        tax_query = 'SELECT tax_year, tax_bill FROM tax_records WHERE bbl=%s AND tax_bill IS NOT NULL ORDER BY bill_date DESC;'
        cursor.execute(tax_query, (self.bbl,))
        row = cursor.fetchone()
        self.property_tax = row[1]
        self.connection_pool.putconn(dbconnection)

    def _init(self):
        super(CondoUnit, self)._init()
        self.cur_fv_l = None
        self.cur_fv_t = None
        self.new_fv_l = None
        self.new_fv_t = None
        self.curavl = None
        self.curavt = None
        self.curexl = None
        self.curext = None
        self.curavl_a = None
        self.curavt_a = None
        self.curexl_a = None
        self.curext_a = None
        self.tn_avt = None
        self.tn_avl = None
        self.tn_ext = None
        self.tn_avl_a = None
        self.tn_avt_a = None
        self.tn_exl_a = None
        self.tn_ext_a = None
        self.fn_avl = None
        self.fn_avt = None
        self.fn_exl = None
        self.fn_avl_a = None
        self.fn_avt_a = None
        self.fn_exl_a = None
        self.fn_ext_a = None
        self.property_tax = None
        self.sales = []

    def get_last_sale(self):
        if len(self.sales) > 0:
            return self.sales[-1]
        else:
            return None

    def get_json(self):
        schema = CondoUnitSchema()
        return schema.dump(self)


class PropertySale:
    def __init__(self, price=None, date=None, seller=None, buyer=None):
        self.price = price
        self.date = date
        self.seller = seller
        self.buyer = buyer

class MailingAddress:
    def __init__(self, bbl, connection_pool=None):
        self.bbl = bbl
        self.address = None
        self.connection_pool = connection_pool

    def _load(self):
        if self.connection_pool is None:
            return None
        query = '''SELECT bbl, address FROM mailing_addresses WHERE bbl=%s'''
        dbconnection = self.connection_pool.getconn()

        cursor = dbconnection.cursor()
        cursor.execute(query, (self.bbl,))
        row = cursor.fetchone()
        if row is not None:
            self.address = row[1]

    def get_json(self):

        if self.address is None:
            self._load()

        schema = MailingAddressSchema()
        return schema.dump(self)
