import sys
from common import comparesettlementdata_lib, comparebybank_lib
function = sys.argv[1]
hospital_id = sys.argv[2]

if __name__ == '__main__':
    if function == 'comparesettlementdata':
        comparesettlementdata_lib(hospital_id)
    if function == 'comparebybank':
        comparebybank_lib(hospital_id)
