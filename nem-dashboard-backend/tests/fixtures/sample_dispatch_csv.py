"""Sample NEM dispatch CSV content for testing"""
import io
import zipfile

# Valid NEM dispatch CSV format
SAMPLE_DISPATCH_CSV = b'''C,NEMP.WORLD,,DISPATCH,UNIT_SCADA,1
I,DISPATCH,UNIT_SCADA,1,SETTLEMENTDATE,DUID,SCADAVALUE,LASTCHANGED
D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",BAYSW1,350.5,"2025/01/15 10:30:05"
D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",AGLHAL,94.2,"2025/01/15 10:30:05"
D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",ARWF1,185.0,"2025/01/15 10:30:05"
D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",ERGT01,0.0,"2025/01/15 10:30:05"
D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",LOYARD1,220.8,"2025/01/15 10:30:05"
C,END OF REPORT,,,
'''

# CSV with empty/malformed values
SAMPLE_DISPATCH_EMPTY_VALUES = b'''D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",TESTGEN,,"2025/01/15 10:30:05"
D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",TESTGEN2,   ,"2025/01/15 10:30:05"
'''

# CSV with invalid float
SAMPLE_DISPATCH_INVALID_FLOAT = b'''D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",TESTGEN,INVALID,"2025/01/15 10:30:05"
'''

# CSV with no DISPATCH,UNIT_SCADA records
SAMPLE_DISPATCH_NO_RECORDS = b'''C,NEMP.WORLD,,OTHER,DATA,1
I,OTHER,DATA,1,COL1,COL2
D,OTHER,DATA,1,val1,val2
C,END OF REPORT,,,
'''

# CSV with malformed lines (too few columns)
SAMPLE_DISPATCH_MALFORMED = b'''D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",GOOD1,100.0,"2025/01/15 10:30:05"
D,DISPATCH,UNIT_SCADA,SHORT
D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:35:00",GOOD2,200.0,"2025/01/15 10:30:05"
'''

# CSV with negative values (valid in NEM)
SAMPLE_DISPATCH_NEGATIVE = b'''D,DISPATCH,UNIT_SCADA,1,"2025/01/15 10:30:00",BATTERY1,-50.5,"2025/01/15 10:30:05"
'''

# HTML directory listing for file parsing
SAMPLE_DIRECTORY_HTML = '''<!DOCTYPE html>
<html>
<head><title>Index of /REPORTS/CURRENT/Dispatch_SCADA/</title></head>
<body>
<h1>Index of /REPORTS/CURRENT/Dispatch_SCADA/</h1>
<pre>
<a href="PUBLIC_DISPATCHSCADA_202501151020_0000000123456789.zip">PUBLIC_DISPATCHSCADA_202501151020_0000000123456789.zip</a>
<a href="PUBLIC_DISPATCHSCADA_202501151025_0000000123456790.zip">PUBLIC_DISPATCHSCADA_202501151025_0000000123456790.zip</a>
<a href="PUBLIC_DISPATCHSCADA_202501151030_0000000123456791.zip">PUBLIC_DISPATCHSCADA_202501151030_0000000123456791.zip</a>
</pre>
</body>
</html>
'''

SAMPLE_DIRECTORY_HTML_EMPTY = '''<!DOCTYPE html>
<html><head><title>Empty</title></head><body></body></html>
'''

SAMPLE_DIRECTORY_HTML_SINGLE = '''<a href="PUBLIC_DISPATCHSCADA_202501151030_0000000123456789.zip">file</a>'''


def create_sample_dispatch_zip(csv_content: bytes = SAMPLE_DISPATCH_CSV) -> bytes:
    """Create a sample NEM dispatch ZIP file for testing"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('PUBLIC_DISPATCHSCADA_202501151030.CSV', csv_content)
    return buffer.getvalue()


def create_empty_zip() -> bytes:
    """Create a ZIP file with no CSV files"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as zf:
        zf.writestr('readme.txt', 'No CSV here')
    return buffer.getvalue()


def create_zip_with_multiple_csvs() -> bytes:
    """Create a ZIP with multiple CSV files"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('PUBLIC_DISPATCHSCADA_202501151030_1.CSV', SAMPLE_DISPATCH_CSV)
        zf.writestr('PUBLIC_DISPATCHSCADA_202501151030_2.CSV', b'other content')
    return buffer.getvalue()


def create_nested_archive_zip(csv_content: bytes = SAMPLE_DISPATCH_CSV, num_intervals: int = 2) -> bytes:
    """Create a nested ZIP archive (ZIP of ZIPs) like NEMWEB historical archives.

    NEMWEB daily dispatch archives contain inner ZIPs for each 5-minute interval,
    and each inner ZIP contains a CSV file.
    """
    outer_buffer = io.BytesIO()
    with zipfile.ZipFile(outer_buffer, 'w', zipfile.ZIP_DEFLATED) as outer_zf:
        for i in range(num_intervals):
            # Create inner ZIP with CSV
            inner_buffer = io.BytesIO()
            with zipfile.ZipFile(inner_buffer, 'w', zipfile.ZIP_DEFLATED) as inner_zf:
                inner_zf.writestr(f'PUBLIC_DISPATCHSCADA_20250115{1030+i*5:04d}.CSV', csv_content)

            # Add inner ZIP to outer ZIP
            inner_zip_name = f'PUBLIC_DISPATCHSCADA_20250115{1030+i*5:04d}_0000000123456{789+i}.zip'
            outer_zf.writestr(inner_zip_name, inner_buffer.getvalue())

    return outer_buffer.getvalue()
