from astor_schemas import *
import math
from astor_square_utils import *

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
