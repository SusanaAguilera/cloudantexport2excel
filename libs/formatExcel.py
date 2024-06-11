# -----------------------------------------------------------
# class to put format on a excel file using pandas
#
# (C) 2024 Ivan Gustavo Ortiz García
# Released under MIT License (MIT)
# GH Repository https://github.com/SusanaAguilera/cloudantexport2excel 
# -----------------------------------------------------------
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


class excelFormatting:
    """
        excelFormatting used put some format on excel files.
        
        Attributes:
            writer [dict]\n
            worksheet [dict]\n
            workbook [dict]\n
        Methods:
            createWriter(self, filename="", sheetName="",dataFrame={}): Creates an Excel File and setup the writer.\n
            setSheet(self, sheetName=""): Set the Sheet to be edited.\n
            setHeaderFormat(self, headerFormat={}, dataFrame={}): Set the Header's format from a target sheet.\n
            closeWriter(self): Close the writer to Stop the editing.
    """ 
    
    writer={};
    worksheet={};
    workbook={};
        
    def createWriter(self, filename="", sheetName="",dataFrame={}):
        """
            Creates an Excel File and setup the writer.\n
            Args:
                filename : String
                sheetName : String
                dataFrame : Dict
            Returns:
                Boolean
        """
        try:
            self.writer = pd.ExcelWriter(f"{filename}.xlsx", engine="xlsxwriter");
            dataFrame.to_excel(self.writer, sheet_name=sheetName, startrow=0, header=True)
            return True;
        except Exception as e:
            print('Error at [libs][excelFormatting][createWriter].-')
            print(e)
            return (False);
        
    def setSheet(self, sheetName=""):
        """
            Set the Sheet to be edited.\n
            Args:
                sheetName : String
            Returns:
                Boolean
        """
        try:
            self.workbook = self.writer.book;
            self.worksheet = self.writer.sheets[sheetName];
            return True;
        except Exception as e:
            print('Error at [libs][excelFormatting][setSheet].-')
            print(e)
            return (False);
    
    def setHeaderFormat(self, headerFormat={}, dataFrame={}):
        """
            Set the Header's format from a target sheet.\n
            Args:
                headerFormat : Dict
                dataFrame : {}
            Returns:
                Boolean
        """
        try:
            header_format = self.workbook.add_format(headerFormat);
            # Write the column headers with the defined format.
            for col_num, value in enumerate(dataFrame.columns.values):
                self.worksheet.write(0, col_num + 1, value, header_format)
            return True;
        except Exception as e:
            print('Error at [libs][excelFormatting][setHeaderFormat].-')
            print(e)
            return (False);
        
    def closeWriter(self):
        """
            Close the writer to Stop the editing\n
            Args:
            
            Returns:
                Boolean
        """
        try:
            self.writer.close();
            return True;
        except Exception as e:
            print('Error at [libs][excelFormatting][closeWriter].-')
            print(e)
            return (False);
        