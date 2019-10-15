from marshmallow import Schema, fields

class UnitSchema(Schema):
    id = fields.Str()
    gr_sqft = fields.Int()
    aptno = fields.Str()

class CondoUnitSchema(UnitSchema):
    bbl = fields.Str()
    cur_fv_l = fields.Int()
    cur_fv_t = fields.Int()
    new_fv_l = fields.Int()
    new_fv_t = fields.Int()
    curavl = fields.Int()
    curavt = fields.Int()
    curexl = fields.Int()
    curext = fields.Int()
    curavl_a = fields.Int()
    curavt_a = fields.Int()
    curexl_a = fields.Int()
    curext_a = fields.Int()
    tn_avt = fields.Int()
    tn_avl = fields.Int()
    tn_ext = fields.Int()
    tn_avl_a = fields.Int()
    tn_avt_a = fields.Int()
    tn_exl_a = fields.Int()
    tn_ext_a = fields.Int()
    fn_avl = fields.Int()
    fn_avt = fields.Int()
    fn_exl = fields.Int()
    fn_avl_a = fields.Int()
    fn_avt_a = fields.Int()
    fn_exl_a = fields.Int()
    fn_ext_a = fields.Int()
    property_tax = fields.Float()
    sales = fields.List(fields.Nested('PropertySaleSchema'))

class PropertySaleSchema(Schema):
    price = fields.Float()
    date = fields.Date()
    seller = fields.Str()
    buyer = fields.Str()

class BuildingSchema(Schema):
    bbl = fields.Str()
    address = fields.Str()
    lotarea = fields.Int()
    bldgarea = fields.Int()
    comarea = fields.Int()
    resarea = fields.Int()
    officearea = fields.Int()
    retailarea = fields.Int()
    garagearea = fields.Int()
    strgearea = fields.Int()
    factryarea = fields.Int()
    otherarea = fields.Int()
    gr_sqft = fields.Int()
    property_tax = fields.Float()
    numfloors = fields.Int()
    unitsres = fields.Int()
    unitstotal = fields.Int()
    yearbuilt = fields.Int()
    yearalter1 = fields.Int()
    yearalter2 = fields.Int()
    xcoord = fields.Int()
    ycoord = fields.Int()
    nearby_buildings = fields.List(fields.Nested('BuildingSchema'))
    sales = fields.List(fields.Nested('PropertySaleSchema'))
    units = fields.List(fields.Nested('UnitSchema'))
    full_address = fields.Str()

class ApartmentBuildingSchema(BuildingSchema):
    cur_fv_l = fields.Float()
    cur_fv_t = fields.Float()
    new_fv_l = fields.Float()
    new_fv_t = fields.Float()
    curavl = fields.Float()
    curavt = fields.Float()
    curexl = fields.Float()
    curext = fields.Float()
    curavl_a = fields.Float()
    curavt_a = fields.Float()
    curexl_a = fields.Float()
    curext_a = fields.Float()
    tn_avt = fields.Float()
    tn_avl = fields.Float()
    tn_ext = fields.Float()
    tn_avl_a = fields.Float()
    tn_avt_a = fields.Float()
    tn_exl_a = fields.Float()
    tn_ext_a = fields.Float()
    fn_avl = fields.Float()
    fn_avt = fields.Float()
    fn_exl = fields.Float()
    fn_avl_a = fields.Float()
    fn_avt_a = fields.Float()
    fn_exl_a = fields.Float()
    fn_ext_a = fields.Float()


class ComparableSchema(Schema):

    neighborhood = fields.Str()
    building_class = fields.Str()
    borough_block_lot = fields.Str()
    address = fields.Str()
    year_built = fields.Int()
    total_units = fields.Int()
    gross_square_feet = fields.Float()
    estimated_gross_income = fields.Float()
    gross_income_per_square_foot = fields.Float()
    estimated_expense = fields.Float()
    expense_per_square_foot = fields.Float()
    net_operating_income = fields.Float()
    net_operating_income_per_square_foot = fields.Float()
    full_market_value = fields.Float()
    market_value_per_square_foot = fields.Float()
    net_present_value = fields.Float()
    net_present_value_per_square_foot = fields.Float()
    annual_tax = fields.Float()
    comparable_of = fields.Str()
    full_address = fields.Str()


class PropertyTaxAnalysisSchema(ComparableSchema):

    last_year_total_market_value = fields.Float()
    this_year_total_market_value = fields.Float()
    last_year_assessed_value = fields.Float()
    this_year_assessed_value = fields.Float()
    last_year_transitional_assessed_value = fields.Float()
    this_year_transitional_assessed_value = fields.Float()
    last_year_annual_tax = fields.Float()
    this_year_annual_tax = fields.Float()

class UnitAndBuildingTaxAnalysisSchema(Schema):
    unit_tax_analysis = fields.Nested('PropertyTaxAnalysisSchema')
    building_tax_analysis = fields.Nested('PropertyTaxAnalysisSchema')

class CondoBuildingSchema(BuildingSchema):
    units = fields.List(fields.Nested('CondoUnitSchema'))

class MailingAddressSchema(Schema):
    bbl = fields.Str()
    address = fields.Str()

