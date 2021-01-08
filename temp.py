import re
a = """
'Dear Patient,
The Pre-Authorization request has been submitted to <<insurerTPA>>. 
Contact TPA Desk for further information - +91 9667251365
<<hospitalID>>'
"""
b = re.findall(r"(?<=<<).*(?=>>)", a)
pass