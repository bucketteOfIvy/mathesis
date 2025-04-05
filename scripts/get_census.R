library(ipumsr)
library(dplyr)
library(sf)
library(tidycensus)

rel_file <- 'nhgis0005_shape/nhgis0005_shapefile_tl2008_us_tract_1940.zip'

cens1940 <- ipumsr::read_nhgis('../data/nhgis0005_csv.zip')
cens1940shp <- ipumsr::read_ipums_sf('../data/nhgis0005_shape.zip', 
                                file_select=rel_file) |>
  select(GISJOIN)

cens1940 |> merge(cens1940shp, by='GISJOIN')

# check what is missing
cens1940 |> filter(!GISJOIN %in% cens1940shp$GISJOIN)


rename_map <- c(
  # Metadata we don't want to rename
  'GISJOIN'='GISJOIN',   'YEAR'='YEAR',
  'STATE'='STATE',       'STATEA'='STATEA',
  'COUNTY'='COUNTY',     'COUNTYA'='COUNTYA',
  'PRETRACTA'='PRETRACTA', 'TRACTA'='TRACTA',   
  'POSTTRCTA'='POSTTRCTA', 'AREANAME'='AREANAME',
  'geometry'='geometry',
  # NT1: Population 
  'BUB001' = "popTotal",
  # NT2: Population by Race
  'BUQ001' = "popWhite", 'BUQ002' = "popNonwhite",
  # NT4: Negro Population
  'BVG001' = "negroPopTotal",
  # NT5: Occupied Dwelling Units
  'BVP001' = "occDwellingUnitsTotal",
  # NT15: Persons 25 Years and Over by Sex and Years of School Completed
  'BUH001' = "maleNoSchool",       'BUH002' = "maleElem1to4",
  'BUH003' = "maleElem5to6",       'BUH004' = "maleElem7to8",
  'BUH005' = "maleHS1to3",         'BUH006' = "maleHS4",
  'BUH007' = "maleCollege1to3",    'BUH008' = "maleCollege4plus",
  'BUH009' = "maleNoSchoolReport",       'BUH010' = "femaleNoSchool",
  'BUH011' = "femaleElem1to4",     'BUH012' = "femaleElem5to6",
  'BUH013' = "femaleElem7to8",     'BUH014' = "femaleHS1to3",
  'BUH015' = "femaleHS4",          'BUH016' = "femaleCollege1to3",
  'BUH017' = "femaleCollege4plus", 'BUH018' = "femaleNoSchoolReport",
  # NT16: Persons 25 Years and Over by Sex by Median Years of School Completed
  'BUI001' = "maleMedianYears", 'BUI002' = "femaleMedianYears",
  # NT21: Sex by Labor Force Status [Persons 14 Years and Over]
  'BUW001' = "maleInLabor",   'BUW002' = "maleNotInLabor",
  'BUW003' = "femaleInLabor", 'BUW004' = "femaleNotInLabor",
  # NT22: Sex by Detailed Labor Force Status [Persons 14 Years and Over]
  'BUX001' = "maleEmployed",           'BUX002' = "malePubEmergWork",
  'BUX003' = "maleSeekWork",           'BUX004' = "maleNotInLaborHouse",
  'BUX005' = "maleNotInLaborSchool",   'BUX006' = "maleNotInLaborUnable",
  'BUX007' = "maleNotInLaborInst",     'BUX008' = "maleNotInLaborOther",
  'BUX009' = "femaleEmployed",         'BUX010' = "femalePubEmergWork",
  'BUX011' = "femaleSeekWork",         'BUX012' = "femaleNotInLaborHouse",
  'BUX013' = "femaleNotInLaborSchool", 'BUX014' = "femaleNotInLaborUnable",
  'BUX015' = "femaleNotInLaborInst",   'BUX016' = "femaleNotInLaborOther",
  # NT25: Population by Sex by Occupational Group [Employed Persons]
  'BU0001' = "maleProf",       'BU0002' = "maleSemiProf",
  'BU0003' = "maleProp",       'BU0004' = "maleClerical",
  'BU0005' = "maleCrafts",     'BU0006' = "maleOperatives",
  'BU0007' = "maleDomestic",   'BU0008' = "maleService",
  'BU0009' = "maleLabor",      'BU0010' = "maleNoOccReport",
  'BU0011' = "femaleProf",     'BU0012' = "femaleSemiProf",
  'BU0013' = "femaleProp",     'BU0014' = "femaleClerical",
  'BU0015' = "femaleCrafts",   'BU0016' = "femaleOperatives",
  'BU0017' = "femaleDomestic", 'BU0018' = "femaleService",
  'BU0019' = "femaleLabor",    'BU0020' = "femaleNoOccReport",
  # NT30: Dwelling Units by Type of Structure
  'BU6001' = "fam1Detach",     'BU6002' = "fam1Attach",
  'BU6003' = "fam2SideBySide", 'BU6004' = "fam2Other",
  'BU6005' = "fam3",           'BU6006' = "fam4",
  'BU6007' = "fam1to4WithBiz", 'BU6008' = "fam5to9",
  'BU6009' = "fam10to19",      'BU6010' = "fam20plus",
  'BU6011' = "otherStruct", 
  # NT32: Occupied Dwelling Units
  'BU8001' = "underPt51",   'BU8002' = "pt51to75",
  'BU8003' = "pt76to1",     'BU8004' = "pt1to1p5",    
  'BU8005' = "pt1p5to2",    'BU8006' = "pt2plus",     
  'BU8007' = "noReportRoom",
  # NT33: Tenant-Occupied Dwelling Units
  'BU9001' = "tenUnderPt51", 'BU9002' = "tenPt51to75",
  'BU9003' = "tenPt76to1",   'BU9004' = "tenPt1to1p5",
  'BU9005' = "tenPt1p5to2",  'BU9006' = "tenPt2plus",
  'BU9007' = "tenNoReport",
  # NT43: Dwelling Units by State of Repair
  'BVK001' = "noMajorRepair", 'BVK002' = "majorRepair",
  'BVK003' = "repairNoReport",
  # NT45: Occupied Dwelling Units by Radio Ownership
  'BVM001' = "radio", 'BVM002' = "noRadio",
  'BVM003' = "radioNoReport",
  # NT46: Occupied Dwelling Units by Refrigeration
  'BVN001' = "refrigMech",  'BVN002' = "refrigIce",
  'BVN003' = "refrigOther", 'BVN004' = "refrigNone",
  'BVN005' = "refrigNoReport",
  # NT47: Occupied Dwelling Units by Heating
  'BVO001' = "heatCentral", 'BVO002' = "heatNoCentral",
  'BVO003' = "heatNoReport"
)


# rename_map defined elsewhere, just makes variables human readable
cens1940 <- cens1940 |> merge(cens1940shp, by='GISJOIN')
colnames(cens1940) <- rename_map[colnames(cens1940)]

# TIME TO DO VARIABLE REFACTORING
cens1940 <- cens1940 |>
  mutate(
    whiteP    = round(100 * popWhite / popTotal, 2),
    nonWhiteP = round(100 * popNonwhite / popTotal, 2),
    blackP    = round(100 * negroPopTotal / popTotal, 2),
    noSchool   = maleNoSchool + femaleNoSchool,
    elementary = maleElem1to4 + maleElem5to6 + maleElem7to8 + femaleElem1to4 + femaleElem5to6 + 
      femaleElem7to8,
    highSchool = maleHS1to3 + maleHS4 + femaleHS1to3 + femaleHS4,
    college    = maleCollege1to3 + maleCollege4plus + femaleCollege1to3 + femaleCollege4plus,
    noReportSchool = maleNoSchoolReport + femaleNoSchoolReport,
    medMaleSchoolingYears = maleMedianYears,
    medFemaleSchoolingYears = femaleMedianYears,
    pop14Plus = maleInLabor + maleNotInLabor + femaleInLabor + femaleNotInLabor
  ) |>
  mutate(
    employed = maleEmployed + femaleEmployed,
    employedP = round(100 * (maleEmployed + femaleEmployed) / pop14Plus, 2),
    seekWorkP = round(100 * (maleSeekWork + femaleSeekWork) / pop14Plus, 2),
    notInLaborP = round(100 * (maleNotInLabor + femaleNotInLabor) / pop14Plus, 2),
  ) |>  
  mutate(
    profP = round(100 * (maleProf + femaleProf) / employed, 2),
    semiProfP = round(100 * (maleSemiProf + femaleSemiProf) / employed, 2),
    propP = round(100 * (maleProp + femaleProp) / employed, 2),
    clericalP = round(100 * (maleClerical + femaleClerical) / employed, 2),
    craftsP = round(100 * (maleCrafts + femaleCrafts) / employed, 2),
    operativesP = round(100 * (maleOperatives + femaleOperatives) / employed, 2),
    domesticP = round(100 * (maleDomestic + femaleDomestic) / employed, 2),
    serviceP = round(100 * (maleService + femaleService) / employed, 2),
    laborP = round(100 * (maleLabor + femaleLabor) / 100, 2)
  ) |>
  select(GISJOIN, YEAR, STATE, STATEA, COUNTY, COUNTYA, PRETRACTA, TRACTA, POSTTRCTA, AREANAME, geometry, whiteP, nonWhiteP, blackP, 
         noSchool, elementary, highSchool, college, 
         noReportSchool, medMaleSchoolingYears, 
         medFemaleSchoolingYears, employedP, 
         seekWorkP, notInLaborP, profP, semiProfP, 
         propP, clericalP, craftsP, operativesP, 
         domesticP, serviceP, laborP, fam1Detach, 
         fam1Attach, fam2SideBySide, fam2Other, fam3, 
         fam4, fam1to4WithBiz, fam5to9, fam10to19, 
         fam20plus, otherStruct, underPt51, pt51to75, 
         pt76to1, pt1to1p5, pt1p5to2, pt2plus, 
         noReportRoom, tenUnderPt51, tenPt51to75, tenPt76to1, 
         tenPt1to1p5, tenPt1p5to2, tenPt2plus, tenNoReport, 
         noMajorRepair, majorRepair, repairNoReport, radio, 
         noRadio, radioNoReport, refrigMech, refrigIce, 
         refrigOther, refrigNone, refrigNoReport, heatCentral, 
         heatNoCentral, heatNoReport) 


# Read in redlining
redlining <- sf::read_sf('../data/shapes/mappinginequality.gpkg') |>
  select(grade) |>
  st_transform('ESRI:102008')

# Merge tracts on redlining neighborhoods
cens1940rdl <- cens1940shp |> 
  sf::st_transform('ESRI:102008') |>
  sf::st_intersection(redlining) 

# Get areas and drop geometries
cens1940rdl$area <- cens1940rdl |>
  sf::st_area()

cens1940rdl <- st_drop_geometry(cens1940rdl)

# Does this do what I claim it does? I don't think so??
# Do manipulations to get percent overlap by GISJOIN by grade
cens1940rdl <- cens1940rdl |> 
  group_by(GISJOIN, grade) |>
  mutate(totAreaGrade=sum(area)) |>
  select(GISJOIN, grade, totAreaGrade) |>
  unique() |>
  ungroup() |>
  group_by(GISJOIN) |>
  mutate(totArea=sum(totAreaGrade)) |> 
  mutate(propArea=totAreaGrade / totArea) |>
  select(GISJOIN, grade, propArea) |>
  na.exclude()

cens1940rdl <- cens1940rdl |> 
  group_by(GISJOIN) |> 
  mutate(maxPropArea = max(propArea))

# Determine the one with maximal overlap.
# We default to accepting the *highest*
# assigned grade, in case of ties.
# This should only work to weaken observed
# disparities.
cens1940rdl <- cens1940rdl |> filter(
  maxPropArea == propArea
) |>
  dplyr::arrange('GISJOIN', 'grade') |>
  distinct(GISJOIN, .keep_all = TRUE) |>
  select(GISJOIN, grade)

# We lose ~700 observations in the process:
cens1940rdl


cens1940 <- cens1940 |> merge(cens1940rdl, by='GISJOIN', how='inner')

cens1940 <- cens1940 |> filter(grade != 'E')

# save progress
cens1940 |> sf::st_write('../data/shapes/cens1940shapes.shp', append=FALSE)

cens1940 |> head()