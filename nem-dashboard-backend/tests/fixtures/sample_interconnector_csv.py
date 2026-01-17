"""Sample NEM interconnector CSV content for testing"""
import io
import zipfile

# Valid interconnector flow data (matches NEMWEB format with 10+ columns)
# Format: D,INTERCONNECTORRES,runtype,runno,settlementdate,interconnector,meteredmwflow,mwflow,mwloss,marginalvalue,...
SAMPLE_INTERCONNECTOR_CSV = b'''C,NEMP.WORLD,,DISPATCH,INTERCONNECTORRES,1
I,DISPATCH,INTERCONNECTORRES,1,SETTLEMENTDATE,INTERCONNECTORID,METEREDMWFLOW,MWFLOW,MWLOSS,MARGINALVALUE
D,INTERCONNECTORRES,DISPATCH,1,"2025/01/15 10:30:00",N-Q-MNSP1,350.5,355.0,4.5,12.30,extra
D,INTERCONNECTORRES,DISPATCH,1,"2025/01/15 10:30:00",NSW1-QLD1,250.0,252.0,2.0,8.50,extra
D,INTERCONNECTORRES,DISPATCH,1,"2025/01/15 10:30:00",VIC1-NSW1,180.3,182.0,1.7,5.20,extra
D,INTERCONNECTORRES,DISPATCH,1,"2025/01/15 10:30:00",VIC1-SA1,-150.0,-148.0,2.0,3.10,extra
D,INTERCONNECTORRES,DISPATCH,1,"2025/01/15 10:30:00",T-V-MNSP1,50.0,50.5,0.5,2.00,extra
C,END OF REPORT,,,
'''

# Interconnector with negative flow (reverse direction)
SAMPLE_INTERCONNECTOR_NEGATIVE = b'''D,INTERCONNECTORRES,DISPATCH,1,"2025/01/15 10:30:00",VIC1-SA1,-250.5,-248.0,2.5,8.50,extra
'''

# No interconnector records
SAMPLE_INTERCONNECTOR_NO_RECORDS = b'''C,NEMP.WORLD,,OTHER,DATA,1
D,OTHER,DATA,val1,val2
C,END OF REPORT,,,
'''

# Malformed interconnector data (some lines have fewer than 10 columns)
SAMPLE_INTERCONNECTOR_MALFORMED = b'''D,INTERCONNECTORRES,DISPATCH,1,"2025/01/15 10:30:00",GOOD1,100.0,102.0,2.0,5.0,extra
D,INTERCONNECTORRES,SHORT
D,INTERCONNECTORRES,DISPATCH,1,"2025/01/15 10:30:00",GOOD2,200.0,202.0,2.0,6.0,extra
'''


def create_interconnector_zip(csv_content: bytes = SAMPLE_INTERCONNECTOR_CSV) -> bytes:
    """Create a sample interconnector ZIP file for testing"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('PUBLIC_IRSR_202501151030.CSV', csv_content)
    return buffer.getvalue()
