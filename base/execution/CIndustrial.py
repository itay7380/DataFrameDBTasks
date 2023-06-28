import os
import re
import pandas as pd
import string

_sCurrentDirPath = os.path.dirname(os.path.abspath(__file__))
_s_INDUSTRIAL_FILE_PATH = os.path.join(
        _sCurrentDirPath,
        "resources",
        "annual-enterprise-survey-2020-financial-year-provisional-csv.csv"
    )


class CIndustrial:

    _instance = None

    _RE_GROUPS = re.compile(r'\b[A-S]\d{3}\b')
    _RE_EXCLUDED_GROUPS = r'\b[A-S]\d{3}\b(?=[^()]*\))'
    _RE_GROUPS_OUTSIDE_BRACKETS = re.compile(r'\b[A-S]\d{3}\b(?![^()]*\))')
    _RE_SINGLE_DIVISION = r'division\s+([A-S])'
    _RE_RANGE_DIVISIONS = r'divisions\s+([A-S]-[A-S])'

    _s_VALUE_COLUMN = 'Value'
    _s_ANZSIC06_CODE_COLUMN = 'Industry_code_ANZSIC06'
    _s_INDUSTRY_NAME_COLUMN = 'Industry_name_NZSIOC'
    _s_CATEGORY_COLUMN = 'Variable_category'

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._bInitialized = False
        return cls._instance

    def __init__(self, sIndustrialFilePath: str = _s_INDUSTRIAL_FILE_PATH, sOutputPath: str = "survey-2020-financial.csv"):
        """Process and manipulate the delivered industrial file path

        :param sIndustrialFilePath: Industrial file path to process
        """
        if self._bInitialized:
            return
        if not os.path.exists(sIndustrialFilePath):
            raise ValueError(f"The delivered {sIndustrialFilePath} industrial file is not exists")
        self._bInitialized = True
        self._sOutputPath = sOutputPath
        self._dfIndustrial = pd.read_csv(sIndustrialFilePath)
        self._dTotalGroups = {}
        self._bPopulated = False
        self._bValuesColFixed = False

    def SumValues(self, sIndustry="", sCategory="") -> int:
        if not self._bValuesColFixed:
            self._dfIndustrial[self._s_VALUE_COLUMN] = self._dfIndustrial[self._s_VALUE_COLUMN].apply(lambda x: int(x) if str(x).isdigit() else 0)
            self._bValuesColFixed = True
        if sIndustry == sCategory == "":
            return self._dfIndustrial[self._s_VALUE_COLUMN].sum()
        elif sIndustry == sCategory != "":
            return self._dfIndustrial.loc[
                (self._dfIndustrial[self._s_INDUSTRY_NAME_COLUMN] == sIndustry) &
                (self._dfIndustrial[self._s_CATEGORY_COLUMN] == sCategory), self._s_VALUE_COLUMN].sum()
        elif sIndustry != "" and sCategory == "":
            return self._dfIndustrial.loc[
                self._dfIndustrial[self._s_INDUSTRY_NAME_COLUMN] == sIndustry, self._s_VALUE_COLUMN].sum()
        else:
            # vise versa
            return self._dfIndustrial.loc[
                self._dfIndustrial[self._s_CATEGORY_COLUMN] == sCategory, self._s_VALUE_COLUMN].sum()

    def PopulateGroups(self, sGroupsColumnName: str = "Groups", sExcludedGroupsColumnName: str = "ExcludedGroups"):
        """Task1 - replaces “Industry_code_ANZSIC06” column with the designated groups and excluded groups columns

        :param sGroupsColumnName: The groups column name
        :param sExcludedGroupsColumnName: The excluded groups column name
        """
        if self._bPopulated:
            return
        self._CalculateAllGroups()
        self._ReplaceColumns(sGroupsColumnName, sExcludedGroupsColumnName)
        self.Save(self._sOutputPath)
        self._bPopulated = True

    def Save(self, sFilePath="survey-2020-financial.csv", dataFrame=None):
        """Save the current data frame to csv file

        :param sFilePath: The output file full path location
        """
        if dataFrame is None:
            dataFrame = self._dfIndustrial
        dataFrame.to_csv(sFilePath)

    def CreateLookUp(
            self,
            sIndentifierColumn='Industry_code_ANZSIC06',
            sLookUpColumn='Industry_code_NZSIOC',
            sOutputPath='inverted-survey-2020-financial.csv'
    ):
        if sIndentifierColumn not in self._dfIndustrial:
            raise ValueError(f"The delivered {sIndentifierColumn} column is not exists")
        if sLookUpColumn not in self._dfIndustrial:
            raise ValueError(f"The delivered {sLookUpColumn} column is not exists")
        dfInverted = self._dfIndustrial.groupby(sIndentifierColumn)[sLookUpColumn].apply(lambda x: ",".join(list(set(x)))).reset_index()

        self.Save(sOutputPath, dfInverted)
        return dfInverted

    def _ReplaceColumns(self, sGroupsColumnName, sExcludedGroupsColumnName):
        self._dfIndustrial[sGroupsColumnName] = None
        self._dfIndustrial[sExcludedGroupsColumnName] = None
        for nIndex, sRow in enumerate(self._dfIndustrial[self._s_ANZSIC06_CODE_COLUMN]):
            if "exclud" not in sRow:
                if "division" in sRow:
                    # In this case it should appear in neither of the new column - thus empty value
                    self._AssignGroups(nIndex, sGroupsColumnName, ())
                    self._AssignGroups(nIndex, sExcludedGroupsColumnName, ())
                else:
                    # neither exclude nor division contained
                    tGroups = re.findall(self._RE_GROUPS, sRow)
                    self._AssignGroups(nIndex, sGroupsColumnName, tGroups)
                    self._AssignGroups(nIndex, sExcludedGroupsColumnName, ())
            elif "division" in sRow:
                # both exclude and division contained
                if "divisions" in sRow:
                    # multiple divisions and exclude case
                    sRangeDivisions = re.search(self._RE_RANGE_DIVISIONS, sRow).group(1)
                    tExcludeGroups = self._AssignExcludedGroups(nIndex, sRow, sExcludedGroupsColumnName)
                    self._ProcessMultipleDivisionsExclusion(
                        nIndex, tExcludeGroups, sRangeDivisions, sGroupsColumnName
                    )
                else:
                    # single "division" and exclude case
                    sDivision = re.search(self._RE_SINGLE_DIVISION, sRow).group(1)
                    lExcludeGroups = self._AssignExcludedGroups(nIndex, sRow, sExcludedGroupsColumnName)
                    remainsDivisionGroups = self._CalculateRemainDivisionGroups(lExcludeGroups, sDivision)
                    self._AssignGroups(nIndex, sGroupsColumnName, remainsDivisionGroups)
            else:
                # "exclude" contained but division not
                tGroupsOutsideBrackets = re.findall(self._RE_GROUPS_OUTSIDE_BRACKETS, sRow)
                self._AssignGroups(nIndex, sGroupsColumnName, tGroupsOutsideBrackets)
                self._AssignExcludedGroups(nIndex, sRow, sExcludedGroupsColumnName)

        self._dfIndustrial.drop(self._s_ANZSIC06_CODE_COLUMN, axis=1, inplace=True)

    def _CalculateAllGroups(self):
        """Calculates all the existing groups at each division
        """
        if self._dTotalGroups:
            return
        tAnzsic06Unique = tuple(self._dfIndustrial[self._s_ANZSIC06_CODE_COLUMN].unique())
        for sRow in tAnzsic06Unique:
            tGroups = re.findall(self._RE_GROUPS, sRow)
            for sGroup in tGroups:
                setValues = self._dTotalGroups.setdefault(sGroup[0], set())
                setValues.add(sGroup)

    def _AssignExcludedGroups(self, nIndex, sRow, sExcludedGroupsColumnName) -> tuple:
        lExcludeGroups = re.findall(self._RE_EXCLUDED_GROUPS, sRow)
        self._AssignGroups(nIndex, sExcludedGroupsColumnName, lExcludeGroups)
        return tuple(lExcludeGroups)

    def _AssignGroups(self, nIndex, sGroupsColumnName, tGroups):
        self._dfIndustrial.at[nIndex, sGroupsColumnName] = ",".join(tGroups)

    def _CalculateRemainDivisionGroups(self, lExcludeGroups, sDivision):
        """Returns remains division groups tuple
        """
        remainsDivisionGroups = self._dTotalGroups[sDivision] - set(lExcludeGroups)
        return tuple(remainsDivisionGroups)

    def _ProcessMultipleDivisionsExclusion(self, nIndex, tExcludeGroups, sRangeDivisions, sGroupsColumnName):
        startDevision, endDevision = sRangeDivisions.split('-')
        devisionRange = string.ascii_uppercase[
                        string.ascii_uppercase.index(startDevision):string.ascii_uppercase.index(endDevision) + 1
                        ]
        setRemainsDivisionGroups = set()
        for sDivision in devisionRange:
            if not self._IsCharacterAtBeginning(tExcludeGroups, sDivision):
                continue
            setRemainsDivisionGroups.update(set(self._CalculateRemainDivisionGroups(tExcludeGroups, sDivision)))
        self._AssignGroups(nIndex, sGroupsColumnName, setRemainsDivisionGroups)

    def _IsCharacterAtBeginning(self, tStrings: tuple, sChar: str) -> bool:
        for string in tStrings:
            if string.startswith(sChar):
                return True
        return False
