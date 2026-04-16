#!/usr/bin/env python3
"""
Complete COBie Data Quality Assessment Tool
============================================
For Information Management Initiative (IMI)
Based on ISO19650 and NIMA UK Standards

Evaluates COBie facility management data for:
1. Completeness
2. Accuracy  
3. Readiness
4. Usefulness

Returns acceptability status and improvement recommendations
"""

# Install required packages
!pip install pandas openpyxl numpy matplotlib seaborn plotly xlsxwriter -q
!pip install python-dateutil -q

import pandas as pd
import numpy as np
import os
import re
import json
import warnings
warnings.filterwarnings('ignore')
from datetime import datetime, date
import matplotlib.pyplot as plt
import seaborn as sns
from google.colab import files
import io
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import hashlib
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Optional
import math
import sys
import traceback

print("✅ Libraries imported successfully!")
print("=" * 70)

class COBieQualityAnalyzer:
    """
    Comprehensive COBie Data Quality Analysis based on ISO19650 & NIMA UK
    """
    
    def __init__(self):
        # ISO19650 Requirements
        self.iso19650_requirements = {
            "information_requirements": ["LOIN", "AIR", "PIR", "EIR", "OIR"],
            "information_delivery": ["IDP", "TIDP", "MIDP"],
            "collaboration": ["CDE", "RFI", "NCR", "TQ"],
            "digital_twin": ["Asset Information Model", "Asset Management", "Lifecycle"]
        }
        
        # NIMA UK Requirements
        self.nima_requirements = {
            "mandatory_sheets": ["Facility", "Floor", "Space", "Type", "Component", "Contact"],
            "digital_twin_ready": ["Component", "Type", "Space", "System", "Zone"],
            "geospatial_data": ["Coordinate", "Space"],
            "classification_system": "Uniclass2015",
            "data_drops": ["LOD 350", "LOD 400", "LOD 500"]
        }
        
        # COBie Schema Definition (COBie 2.4)
        self.cobie_schema = {
            "Facility": {
                "required": ["Name", "CreatedBy", "CreatedOn", "Category", "ProjectName", "Site"],
                "optional": ["Description", "Phase", "Author", "Organization"],
                "data_types": {"CreatedOn": "datetime", "CreatedBy": "string", "Name": "string"}
            },
            "Floor": {
                "required": ["Name", "CreatedBy", "CreatedOn", "Category", "Elevation", "Height"],
                "optional": ["Description", "ExternalName", "GrossArea", "NetArea"],
                "data_types": {"Elevation": "float", "Height": "float", "CreatedOn": "datetime"}
            },
            "Space": {
                "required": ["Name", "CreatedBy", "CreatedOn", "Category", "FloorName", "Description"],
                "optional": ["RoomTag", "UsableHeight", "GrossArea", "NetArea", "RoomType"],
                "data_types": {"GrossArea": "float", "NetArea": "float", "CreatedOn": "datetime"}
            },
            "Zone": {
                "required": ["Name", "CreatedBy", "CreatedOn", "Category", "SpaceNames"],
                "optional": ["Description", "ParentZone"],
                "data_types": {"CreatedOn": "datetime", "SpaceNames": "list"}
            },
            "Type": {
                "required": ["Name", "CreatedBy", "CreatedOn", "Category", "Manufacturer", "ModelNumber"],
                "optional": ["Description", "WarrantyDuration", "WarrantyGuarantorParts", "WarrantyGuarantorLabor", "ReplacementCost"],
                "data_types": {"CreatedOn": "datetime", "WarrantyDuration": "duration", "ReplacementCost": "currency"}
            },
            "Component": {
                "required": ["Name", "CreatedBy", "CreatedOn", "TypeName", "Space", "Description"],
                "optional": ["SerialNumber", "InstallationDate", "WarrantyStartDate", "TagNumber", "BarCode"],
                "data_types": {"CreatedOn": "datetime", "InstallationDate": "datetime", "WarrantyStartDate": "datetime"}
            },
            "System": {
                "required": ["Name", "CreatedBy", "CreatedOn", "Category", "ComponentNames"],
                "optional": ["Description", "ParentSystem"],
                "data_types": {"CreatedOn": "datetime", "ComponentNames": "list"}
            },
            "Contact": {
                "required": ["Email", "CreatedBy", "CreatedOn", "Category", "Company", "Phone"],
                "optional": ["FirstName", "LastName", "Street", "PostalBox", "Town", "StateRegion", "PostalCode", "Country", "OrganizationCode"],
                "data_types": {"Email": "email", "Phone": "phone", "CreatedOn": "datetime"}
            },
            "Attribute": {
                "required": ["Name", "CreatedBy", "CreatedOn", "Category", "SheetName", "RowName"],
                "optional": ["Value", "Unit", "Description"],
                "data_types": {"CreatedOn": "datetime", "Value": "mixed"}
            },
            "Coordinate": {
                "required": ["Name", "CreatedBy", "CreatedOn", "Category", "SheetName", "RowName"],
                "optional": ["X", "Y", "Z", "Rotation", "Elevation"],
                "data_types": {"X": "float", "Y": "float", "Z": "float", "CreatedOn": "datetime"}
            },
            "Document": {
                "required": ["Name", "CreatedBy", "CreatedOn", "Category", "ApprovalBy", "Stage"],
                "optional": ["Directory", "File", "Description", "Reference"],
                "data_types": {"CreatedOn": "datetime", "ApprovalDate": "datetime"}
            },
            "Issue": {
                "required": ["Name", "CreatedBy", "CreatedOn", "Category", "Risk", "Chance"],
                "optional": ["Description", "Owner", "Mitigation", "Status"],
                "data_types": {"CreatedOn": "datetime", "Risk": "integer", "Chance": "integer"}
            }
        }
        
        # Quality Thresholds (ISO19650 Compliance)
        self.quality_thresholds = {
            "completeness": {
                "overall": 0.70,  # 70% minimum for acceptance
                "mandatory_sheets": 1.00,  # 100% required
                "required_columns": 0.80,
                "critical_fields": 0.90
            },
            "accuracy": {
                "overall": 0.75,
                "data_types": 0.85,
                "formats": 0.80,
                "references": 0.70
            },
            "readiness": {
                "overall": 0.65,
                "standardization": 0.70,
                "classification": 0.60,
                "digital_twin": 0.50
            },
            "usefulness": {
                "overall": 0.60,
                "operational": 0.65,
                "maintenance": 0.55,
                "warranty": 0.45
            }
        }
        
        # Critical fields for facility management
        self.critical_fields = {
            "Facility": ["Name", "CreatedBy", "CreatedOn", "Category"],
            "Type": ["Name", "Category", "Manufacturer", "ModelNumber"],
            "Component": ["Name", "TypeName", "Space", "Description"],
            "Space": ["Name", "FloorName", "Category"],
            "Contact": ["Email", "Company", "Phone"]
        }
        
        # Initialize results storage
        self.assessment_results = {
            "file_info": {},
            "completeness": {},
            "accuracy": {},
            "readiness": {},
            "usefulness": {},
            "issues": [],
            "recommendations": [],
            "overall_score": 0,
            "acceptability": "UNKNOWN"
        }
        
        self.sheets_data = {}
        self.file_name = ""
        
        print("✅ COBie Quality Analyzer Initialized")
        print("📋 Standards: ISO19650 & NIMA UK")
        print("🎯 Dimensions: Completeness, Accuracy, Readiness, Usefulness")
    
    def upload_cobie_file(self):
        """Upload COBie Excel file"""
        print("\n📤 UPLOAD COBie FILE")
        print("-" * 40)
        
        try:
            uploaded = files.upload()
            if not uploaded:
                print("❌ No file uploaded. Using sample data...")
                return self._load_sample_data()
            
            self.file_name = list(uploaded.keys())[0]
            print(f"✅ File uploaded: {self.file_name}")
            print(f"📦 File size: {len(uploaded[self.file_name]) / 1024:.1f} KB")
            
            return uploaded[self.file_name]
            
        except Exception as e:
            print(f"❌ Upload error: {str(e)}")
            return None
    
    def _load_sample_data(self):
        """Create sample COBie data for testing"""
        print("📝 Creating sample COBie data for testing...")
        
        # Create sample dataframes
        sample_data = {}
        
        # Facility
        sample_data['Facility'] = pd.DataFrame({
            'Name': ['Main Building'],
            'CreatedBy': ['System'],
            'CreatedOn': ['2024-01-15T09:00:00'],
            'Category': ['Office Building'],
            'ProjectName': ['Project Alpha'],
            'Site': ['London Site']
        })
        
        # Floor
        sample_data['Floor'] = pd.DataFrame({
            'Name': ['Ground Floor', 'First Floor'],
            'CreatedBy': ['System', 'System'],
            'CreatedOn': ['2024-01-15T09:00:00', '2024-01-15T09:00:00'],
            'Category': ['Office', 'Office'],
            'Elevation': [0.0, 3.5],
            'Height': [3.5, 3.5]
        })
        
        # Space
        sample_data['Space'] = pd.DataFrame({
            'Name': ['Room 101', 'Room 102', 'Corridor G1'],
            'CreatedBy': ['System', 'System', 'System'],
            'CreatedOn': ['2024-01-15T09:00:00', '2024-01-15T09:00:00', '2024-01-15T09:00:00'],
            'Category': ['Office', 'Meeting Room', 'Circulation'],
            'FloorName': ['Ground Floor', 'Ground Floor', 'Ground Floor'],
            'Description': ['Manager Office', 'Conference Room', 'Main Corridor']
        })
        
        # Type
        sample_data['Type'] = pd.DataFrame({
            'Name': ['ACU-001', 'LIGHT-001', 'FIRE-001'],
            'CreatedBy': ['System', 'System', 'System'],
            'CreatedOn': ['2024-01-15T09:00:00', '2024-01-15T09:00:00', '2024-01-15T09:00:00'],
            'Category': ['HVAC', 'Lighting', 'Fire Safety'],
            'Manufacturer': ['ACME Corp', 'BrightLights', 'SafeFire'],
            'ModelNumber': ['AC-5000', 'BL-200', 'FS-100']
        })
        
        # Component
        sample_data['Component'] = pd.DataFrame({
            'Name': ['ACU-101', 'LIGHT-101', 'FIRE-101'],
            'CreatedBy': ['System', 'System', 'System'],
            'CreatedOn': ['2024-01-15T09:00:00', '2024-01-15T09:00:00', '2024-01-15T09:00:00'],
            'TypeName': ['ACU-001', 'LIGHT-001', 'FIRE-001'],
            'Space': ['Room 101', 'Room 101', 'Corridor G1'],
            'Description': ['Air Conditioning Unit', 'LED Light', 'Fire Extinguisher']
        })
        
        # Contact
        sample_data['Contact'] = pd.DataFrame({
            'Email': ['fm@example.com', 'contractor@example.com'],
            'CreatedBy': ['System', 'System'],
            'CreatedOn': ['2024-01-15T09:00:00', '2024-01-15T09:00:00'],
            'Category': ['Facility Manager', 'Contractor'],
            'Company': ['FM Services Ltd', 'BuildRight Ltd'],
            'Phone': ['+44 20 1234 5678', '+44 20 8765 4321']
        })
        
        self.file_name = "Sample_COBie_Data.xlsx"
        self.sheets_data = sample_data
        
        print("✅ Sample data created successfully")
        return b"sample_data"  # Return dummy bytes
    
    def load_cobie_data(self, file_content):
        """Load COBie Excel file into DataFrames"""
        print("\n📊 LOADING COBie DATA")
        print("-" * 40)
        
        try:
            if file_content == b"sample_data":
                print("📋 Using sample data")
                sheets_loaded = list(self.sheets_data.keys())
            else:
                # Read the Excel file
                xls = pd.ExcelFile(io.BytesIO(file_content))
                self.sheets_data = {}
                sheets_loaded = []
                
                print(f"🔍 Found {len(xls.sheet_names)} sheets:")
                print("-" * 30)
                
                for sheet_name in xls.sheet_names:
                    try:
                        df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
                        
                        # Clean column names (remove extra spaces, etc.)
                        df.columns = df.columns.str.strip()
                        
                        self.sheets_data[sheet_name] = df
                        sheets_loaded.append(sheet_name)
                        
                        row_count = len(df)
                        col_count = len(df.columns)
                        print(f"✓ {sheet_name:20} | Rows: {row_count:4} | Cols: {col_count:2}")
                        
                    except Exception as e:
                        print(f"✗ {sheet_name:20} | Error: {str(e)[:30]}...")
            
            # Store file info
            self.assessment_results["file_info"] = {
                "filename": self.file_name,
                "sheets_count": len(sheets_loaded),
                "sheets_loaded": sheets_loaded,
                "total_rows": sum(len(df) for df in self.sheets_data.values()),
                "total_columns": sum(len(df.columns) for df in self.sheets_data.values()),
                "load_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            print(f"\n✅ Successfully loaded {len(sheets_loaded)} sheets")
            return True
            
        except Exception as e:
            print(f"❌ Error loading COBie file: {str(e)}")
            print(traceback.format_exc())
            return False
    
    def assess_completeness(self):
        """
        Assess completeness according to ISO19650
        Checks: Mandatory sheets, required columns, data filling
        """
        print("\n📈 ASSESSING COMPLETENESS")
        print("=" * 50)
        
        scores = {}
        findings = []
        recommendations = []
        
        # 1. Check mandatory sheets (NIMA UK requirement)
        mandatory_sheets = self.nima_requirements["mandatory_sheets"]
        available_sheets = list(self.sheets_data.keys())
        
        missing_sheets = []
        for sheet in mandatory_sheets:
            if sheet not in available_sheets:
                missing_sheets.append(sheet)
                findings.append(f"Missing mandatory sheet: {sheet}")
        
        if missing_sheets:
            sheet_completeness = (len(mandatory_sheets) - len(missing_sheets)) / len(mandatory_sheets)
            recommendations.append(f"Add missing sheets: {', '.join(missing_sheets)}")
        else:
            sheet_completeness = 1.0
        
        scores["mandatory_sheets"] = sheet_completeness * 100
        print(f"📋 Mandatory Sheets: {sheet_completeness*100:.1f}%")
        
        # 2. Check required columns for each sheet
        column_scores = []
        for sheet_name, df in self.sheets_data.items():
            if sheet_name in self.cobie_schema:
                required_cols = self.cobie_schema[sheet_name]["required"]
                available_cols = df.columns.tolist()
                
                missing_cols = [col for col in required_cols if col not in available_cols]
                present_cols = [col for col in required_cols if col in available_cols]
                
                col_score = len(present_cols) / len(required_cols) if required_cols else 0
                column_scores.append(col_score)
                
                if missing_cols:
                    findings.append(f"{sheet_name}: Missing columns - {missing_cols[:3]}")  # Show first 3
                
                # Check critical fields
                if sheet_name in self.critical_fields:
                    crit_fields = self.critical_fields[sheet_name]
                    crit_present = [f for f in crit_fields if f in available_cols]
                    crit_score = len(crit_present) / len(crit_fields) if crit_fields else 0
                    
                    if crit_score < 1.0:
                        missing_crit = set(crit_fields) - set(crit_present)
                        findings.append(f"{sheet_name}: Missing critical fields - {list(missing_crit)}")
        
        column_completeness = np.mean(column_scores) if column_scores else 0
        scores["required_columns"] = column_completeness * 100
        print(f"📊 Required Columns: {column_completeness*100:.1f}%")
        
        # 3. Check data filling (non-null values in critical fields)
        data_filling_scores = []
        for sheet_name, df in self.sheets_data.items():
            if sheet_name in self.critical_fields:
                crit_fields = self.critical_fields[sheet_name]
                available_crit = [f for f in crit_fields if f in df.columns]
                
                if available_crit:
                    # Calculate non-null percentage for critical fields
                    non_null_percentages = []
                    for field in available_crit:
                        if field in df.columns:
                            non_null_count = df[field].notna().sum()
                            total_count = len(df)
                            if total_count > 0:
                                non_null_pct = non_null_count / total_count
                                non_null_percentages.append(non_null_pct)
                    
                    if non_null_percentages:
                        sheet_filling_score = np.mean(non_null_percentages)
                        data_filling_scores.append(sheet_filling_score)
                        
                        if sheet_filling_score < 0.9:
                            findings.append(f"{sheet_name}: Low data filling in critical fields ({sheet_filling_score*100:.1f}%)")
        
        data_filling = np.mean(data_filling_scores) if data_filling_scores else 0
        scores["data_filling"] = data_filling * 100
        print(f"📝 Data Filling: {data_filling*100:.1f}%")
        
        # 4. Overall completeness score
        completeness_weights = {
            "mandatory_sheets": 0.40,
            "required_columns": 0.35,
            "data_filling": 0.25
        }
        
        overall_completeness = 0
        for key, weight in completeness_weights.items():
            if key in scores:
                overall_completeness += (scores[key] / 100) * weight
        
        overall_completeness *= 100  # Convert to percentage
        
        # Determine status
        completeness_status = "PASS" if overall_completeness >= self.quality_thresholds["completeness"]["overall"] * 100 else "FAIL"
        
        # Store results
        self.assessment_results["completeness"] = {
            "score": overall_completeness,
            "status": completeness_status,
            "breakdown": scores,
            "findings": findings[:10],  # Limit to 10 findings
            "recommendations": recommendations[:5]  # Limit to 5 recommendations
        }
        
        print(f"\n🏆 COMPLETENESS SCORE: {overall_completeness:.1f}% [{completeness_status}]")
        print(f"📊 Threshold: {self.quality_thresholds['completeness']['overall']*100:.0f}%")
        
        if findings:
            print("\n🔍 Key Findings:")
            for i, finding in enumerate(findings[:5], 1):
                print(f"  {i}. {finding}")
        
        return overall_completeness
    
    def assess_accuracy(self):
        """
        Assess data accuracy according to ISO19650
        Checks: Data types, formats, referential integrity, duplicates
        """
        print("\n🎯 ASSESSING ACCURACY")
        print("=" * 50)
        
        scores = {}
        findings = []
        recommendations = []
        
        # 1. Check data type consistency
        type_scores = []
        for sheet_name, df in self.sheets_data.items():
            if sheet_name in self.cobie_schema:
                schema_types = self.cobie_schema[sheet_name]["data_types"]
                sheet_type_score = 0
                checks = 0
                
                for field, expected_type in schema_types.items():
                    if field in df.columns:
                        checks += 1
                        column_data = df[field].dropna()
                        
                        if len(column_data) > 0:
                            # Check based on expected type
                            if expected_type == "datetime":
                                # Try to parse dates
                                try:
                                    pd.to_datetime(column_data, errors='raise')
                                    sheet_type_score += 1
                                except:
                                    findings.append(f"{sheet_name}.{field}: Invalid date format")
                            
                            elif expected_type == "float":
                                # Check if values can be converted to float
                                numeric_count = column_data.apply(lambda x: str(x).replace('.', '', 1).isdigit()).sum()
                                if numeric_count / len(column_data) > 0.8:
                                    sheet_type_score += 1
                                else:
                                    findings.append(f"{sheet_name}.{field}: Non-numeric values in float field")
                            
                            elif expected_type == "email":
                                # Check email format
                                email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                                valid_emails = column_data.astype(str).str.match(email_pattern).sum()
                                if valid_emails / len(column_data) > 0.8:
                                    sheet_type_score += 1
                                else:
                                    findings.append(f"{sheet_name}.{field}: Invalid email format")
                            
                            else:
                                # For string types, just check if data exists
                                sheet_type_score += 1
                
                if checks > 0:
                    type_score = sheet_type_score / checks
                    type_scores.append(type_score)
        
        data_type_accuracy = np.mean(type_scores) if type_scores else 0
        scores["data_types"] = data_type_accuracy * 100
        print(f"🔤 Data Types: {data_type_accuracy*100:.1f}%")
        
        # 2. Check format consistency
        format_scores = []
        
        # Check date formats (ISO 8601)
        date_columns = []
        for sheet_name, df in self.sheets_data.items():
            date_cols = [col for col in df.columns if any(x in col.lower() for x in ['date', 'created', 'modified', 'installation', 'warranty'])]
            for col in date_cols:
                if col in df.columns:
                    date_columns.append((sheet_name, col))
        
        if date_columns:
            valid_dates = 0
            total_dates = 0
            
            for sheet_name, col in date_columns:
                for value in df[col].dropna():
                    total_dates += 1
                    try:
                        pd.to_datetime(value, errors='raise')
                        valid_dates += 1
                    except:
                        findings.append(f"{sheet_name}.{col}: Invalid date '{value[:20]}...'")
            
            if total_dates > 0:
                format_score = valid_dates / total_dates
                format_scores.append(format_score)
        
        format_accuracy = np.mean(format_scores) if format_scores else 0
        scores["formats"] = format_accuracy * 100
        print(f"📅 Format Consistency: {format_accuracy*100:.1f}%")
        
        # 3. Check referential integrity
        ref_scores = []
        
        # Check Component -> Type reference
        if 'Component' in self.sheets_data and 'Type' in self.sheets_data:
            comp_df = self.sheets_data['Component']
            type_df = self.sheets_data['Type']
            
            if 'TypeName' in comp_df.columns and 'Name' in type_df.columns:
                valid_refs = comp_df['TypeName'].dropna().isin(type_df['Name']).sum()
                total_refs = comp_df['TypeName'].notna().sum()
                
                if total_refs > 0:
                    ref_score = valid_refs / total_refs
                    ref_scores.append(ref_score)
                    
                    if ref_score < 1.0:
                        invalid = comp_df[~comp_df['TypeName'].isin(type_df['Name'])]['TypeName'].unique()[:3]
                        findings.append(f"Component.TypeName: Invalid references - {list(invalid)}")
        
        # Check Space -> Floor reference
        if 'Space' in self.sheets_data and 'Floor' in self.sheets_data:
            space_df = self.sheets_data['Space']
            floor_df = self.sheets_data['Floor']
            
            if 'FloorName' in space_df.columns and 'Name' in floor_df.columns:
                valid_refs = space_df['FloorName'].dropna().isin(floor_df['Name']).sum()
                total_refs = space_df['FloorName'].notna().sum()
                
                if total_refs > 0:
                    ref_score = valid_refs / total_refs
                    ref_scores.append(ref_score)
        
        ref_integrity = np.mean(ref_scores) if ref_scores else 0
        scores["references"] = ref_integrity * 100
        print(f"🔗 Referential Integrity: {ref_integrity*100:.1f}%")
        
        # 4. Check for duplicates
        duplicate_scores = []
        for sheet_name, df in self.sheets_data.items():
            if 'Name' in df.columns:
                total_rows = len(df)
                unique_names = df['Name'].nunique()
                
                if total_rows > 0:
                    dup_score = unique_names / total_rows
                    duplicate_scores.append(dup_score)
                    
                    if dup_score < 0.95:
                        duplicates = df[df['Name'].duplicated()]['Name'].unique()[:3]
                        findings.append(f"{sheet_name}: Duplicate names found - {list(duplicates)}")
                        recommendations.append(f"Remove duplicate entries in {sheet_name} sheet")
        
        duplicate_accuracy = np.mean(duplicate_scores) if duplicate_scores else 0
        scores["duplicates"] = duplicate_accuracy * 100
        
        # 5. Overall accuracy score
        accuracy_weights = {
            "data_types": 0.30,
            "formats": 0.25,
            "references": 0.30,
            "duplicates": 0.15
        }
        
        overall_accuracy = 0
        for key, weight in accuracy_weights.items():
            if key in scores:
                overall_accuracy += (scores[key] / 100) * weight
        
        overall_accuracy *= 100
        
        # Determine status
        accuracy_status = "PASS" if overall_accuracy >= self.quality_thresholds["accuracy"]["overall"] * 100 else "FAIL"
        
        # Store results
        self.assessment_results["accuracy"] = {
            "score": overall_accuracy,
            "status": accuracy_status,
            "breakdown": scores,
            "findings": findings[:10],
            "recommendations": recommendations[:5]
        }
        
        print(f"\n🏆 ACCURACY SCORE: {overall_accuracy:.1f}% [{accuracy_status}]")
        print(f"📊 Threshold: {self.quality_thresholds['accuracy']['overall']*100:.0f}%")
        
        return overall_accuracy
    
    def assess_readiness(self):
        """
        Assess readiness for handover and operations
        Checks: Standardization, classification, Digital Twin readiness
        """
        print("\n⚡ ASSESSING READINESS")
        print("=" * 50)
        
        scores = {}
        findings = []
        recommendations = []
        
        # 1. Standardization check (naming conventions)
        standardization_scores = []
        for sheet_name, df in self.sheets_data.items():
            if 'Name' in df.columns:
                names = df['Name'].dropna().astype(str)
                if len(names) > 0:
                    # Check for consistent naming patterns
                    # Rule 1: No special characters except underscores and hyphens
                    special_chars = names.str.contains(r'[!@#$%^&*()+=<>?/\\|~`]')
                    special_char_rate = 1 - special_chars.mean()
                    
                    # Rule 2: Reasonable length (3-50 characters)
                    length_ok = names.str.len().between(3, 50)
                    length_rate = length_ok.mean()
                    
                    # Combined standardization score
                    std_score = (special_char_rate + length_rate) / 2
                    standardization_scores.append(std_score)
                    
                    if std_score < 0.7:
                        findings.append(f"{sheet_name}: Poor naming standardization ({std_score*100:.1f}%)")
                        recommendations.append(f"Standardize naming convention in {sheet_name}")
        
        standardization = np.mean(standardization_scores) if standardization_scores else 0
        scores["standardization"] = standardization * 100
        print(f"📐 Standardization: {standardization*100:.1f}%")
        
        # 2. Classification check
        classification_scores = []
        classification_patterns = ['Uniclass', 'OmniClass', 'MasterFormat', 'Uniformat', 'EF', 'Pr_', 'Ss_', 'Ac_']
        
        for sheet_name, df in self.sheets_data.items():
            if 'Category' in df.columns:
                categories = df['Category'].dropna().astype(str)
                if len(categories) > 0:
                    # Check for classification codes
                    has_class = categories.str.contains('|'.join(classification_patterns), case=False).mean()
                    classification_scores.append(has_class)
                    
                    if has_class < 0.5:
                        findings.append(f"{sheet_name}: Missing proper classification ({has_class*100:.1f}%)")
        
        classification = np.mean(classification_scores) if classification_scores else 0
        scores["classification"] = classification * 100
        print(f"🏷️ Classification: {classification*100:.1f}%")
        
        # 3. Digital Twin readiness (NIMA UK)
        dt_scores = []
        
        # Check for required sheets
        dt_sheets = self.nima_requirements["digital_twin_ready"]
        dt_present = [sheet for sheet in dt_sheets if sheet in self.sheets_data]
        dt_sheet_score = len(dt_present) / len(dt_sheets) if dt_sheets else 0
        dt_scores.append(dt_sheet_score)
        
        # Check for unique identifiers
        uniqueness_scores = []
        for sheet in dt_present:
            df = self.sheets_data[sheet]
            if 'Name' in df.columns:
                unique_count = df['Name'].nunique()
                total_count = len(df)
                if total_count > 0:
                    uniqueness = unique_count / total_count
                    uniqueness_scores.append(uniqueness)
        
        uniqueness_score = np.mean(uniqueness_scores) if uniqueness_scores else 0
        dt_scores.append(uniqueness_score)
        
        # Check for spatial data
        if 'Coordinate' in self.sheets_data:
            coord_df = self.sheets_data['Coordinate']
            if len(coord_df) > 5:
                dt_scores.append(0.8)  # Bonus for having coordinates
                print(f"📍 Spatial data found: {len(coord_df)} coordinates")
            else:
                findings.append("Coordinate sheet has insufficient data (< 5 entries)")
        
        digital_twin = np.mean(dt_scores) if dt_scores else 0
        scores["digital_twin"] = digital_twin * 100
        print(f"🤖 Digital Twin Readiness: {digital_twin*100:.1f}%")
        
        # 4. Overall readiness score
        readiness_weights = {
            "standardization": 0.35,
            "classification": 0.35,
            "digital_twin": 0.30
        }
        
        overall_readiness = 0
        for key, weight in readiness_weights.items():
            if key in scores:
                overall_readiness += (scores[key] / 100) * weight
        
        overall_readiness *= 100
        
        # Determine status
        readiness_status = "PASS" if overall_readiness >= self.quality_thresholds["readiness"]["overall"] * 100 else "FAIL"
        
        # Store results
        self.assessment_results["readiness"] = {
            "score": overall_readiness,
            "status": readiness_status,
            "breakdown": scores,
            "findings": findings[:10],
            "recommendations": recommendations[:5]
        }
        
        print(f"\n🏆 READINESS SCORE: {overall_readiness:.1f}% [{readiness_status}]")
        print(f"📊 Threshold: {self.quality_thresholds['readiness']['overall']*100:.0f}%")
        
        return overall_readiness
    
    def assess_usefulness(self):
        """
        Assess usefulness for facility management
        Checks: Operational relevance, maintenance info, warranty data
        """
        print("\n💡 ASSESSING USEFULNESS")
        print("=" * 50)
        
        scores = {}
        findings = []
        recommendations = []
        
        # 1. Operational relevance
        operational_scores = []
        
        # Check Component sheet for operational data
        if 'Component' in self.sheets_data:
            comp_df = self.sheets_data['Component']
            operational_fields = ['InstallationDate', 'SerialNumber', 'TagNumber', 'BarCode']
            
            op_present = [field for field in operational_fields if field in comp_df.columns]
            op_score = len(op_present) / len(operational_fields) if operational_fields else 0
            operational_scores.append(op_score)
            
            if op_score < 0.5:
                findings.append("Component sheet lacks operational data (serial numbers, installation dates)")
                recommendations.append("Add operational data to Component sheet")
        
        # Check for maintenance information
        if 'Attribute' in self.sheets_data:
            attr_df = self.sheets_data['Attribute']
            maintenance_keywords = ['maintenance', 'service', 'inspection', 'calibration', 'cleaning']
            
            maintenance_attrs = 0
            total_attrs = len(attr_df)
            
            if total_attrs > 0:
                for keyword in maintenance_keywords:
                    maintenance_attrs += attr_df['Category'].astype(str).str.contains(keyword, case=False).sum()
                
                maint_score = min(maintenance_attrs / 10, 1.0)  # Cap at 1.0
                operational_scores.append(maint_score)
                
                if maint_score < 0.3:
                    findings.append("Limited maintenance information in Attribute sheet")
        
        operational = np.mean(operational_scores) if operational_scores else 0
        scores["operational"] = operational * 100
        print(f"🔧 Operational Relevance: {operational*100:.1f}%")
        
        # 2. Maintenance readiness
        maintenance_scores = []
        
        # Check for maintenance schedule (Job sheet)
        if 'Job' in self.sheets_data:
            job_df = self.sheets_data['Job']
            if len(job_df) > 0:
                maintenance_scores.append(0.8)  # Bonus for having maintenance jobs
                print(f"🔧 Maintenance jobs: {len(job_df)} entries")
            else:
                findings.append("Job sheet is empty")
        
        # Check for spare parts
        if 'Spare' in self.sheets_data:
            spare_df = self.sheets_data['Spare']
            if len(spare_df) > 0:
                maintenance_scores.append(0.7)
                print(f"🔧 Spare parts: {len(spare_df)} entries")
        
        maintenance = np.mean(maintenance_scores) if maintenance_scores else 0
        scores["maintenance"] = maintenance * 100
        print(f"🛠️ Maintenance Info: {maintenance*100:.1f}%")
        
        # 3. Warranty information
        warranty_scores = []
        
        if 'Type' in self.sheets_data:
            type_df = self.sheets_data['Type']
            warranty_fields = ['WarrantyDuration', 'WarrantyGuarantorParts', 'WarrantyGuarantorLabor']
            
            warranty_present = [field for field in warranty_fields if field in type_df.columns]
            warranty_complete = 0
            
            for field in warranty_present:
                if field in type_df.columns:
                    non_null = type_df[field].notna().sum()
                    total = len(type_df)
                    if total > 0:
                        warranty_complete += non_null / total
            
            warranty_score = warranty_complete / len(warranty_fields) if warranty_fields else 0
            warranty_scores.append(warranty_score)
            
            if warranty_score < 0.3:
                findings.append("Limited warranty information in Type sheet")
                recommendations.append("Add warranty details to Type sheet")
        
        warranty = np.mean(warranty_scores) if warranty_scores else 0
        scores["warranty"] = warranty * 100
        print(f"📄 Warranty Info: {warranty*100:.1f}%")
        
        # 4. Asset criticality
        criticality_scores = []
        
        if 'Attribute' in self.sheets_data:
            attr_df = self.sheets_data['Attribute']
            criticality_keywords = ['criticality', 'risk', 'priority', 'importance']
            
            critical_attrs = 0
            for keyword in criticality_keywords:
                critical_attrs += attr_df['Category'].astype(str).str.contains(keyword, case=False).sum()
            
            if len(attr_df) > 0:
                crit_score = min(critical_attrs / 5, 1.0)
                criticality_scores.append(crit_score)
        
        # Check Impact sheet
        if 'Impact' in self.sheets_data:
            impact_df = self.sheets_data['Impact']
            if len(impact_df) > 0:
                criticality_scores.append(0.6)
        
        criticality = np.mean(criticality_scores) if criticality_scores else 0
        scores["criticality"] = criticality * 100
        print(f"⚠️ Asset Criticality: {criticality*100:.1f}%")
        
        # 5. Overall usefulness score
        usefulness_weights = {
            "operational": 0.35,
            "maintenance": 0.30,
            "warranty": 0.20,
            "criticality": 0.15
        }
        
        overall_usefulness = 0
        for key, weight in usefulness_weights.items():
            if key in scores:
                overall_usefulness += (scores[key] / 100) * weight
        
        overall_usefulness *= 100
        
        # Determine status
        usefulness_status = "PASS" if overall_usefulness >= self.quality_thresholds["usefulness"]["overall"] * 100 else "FAIL"
        
        # Store results
        self.assessment_results["usefulness"] = {
            "score": overall_usefulness,
            "status": usefulness_status,
            "breakdown": scores,
            "findings": findings[:10],
            "recommendations": recommendations[:5]
        }
        
        print(f"\n🏆 USEFULNESS SCORE: {overall_usefulness:.1f}% [{usefulness_status}]")
        print(f"📊 Threshold: {self.quality_thresholds['usefulness']['overall']*100:.0f}%")
        
        return overall_usefulness
    
    def calculate_overall_score(self):
        """Calculate overall COBie quality score with weights"""
        print("\n📊 CALCULATING OVERALL SCORE")
        print("=" * 50)
        
        # Weights based on ISO19650 importance
        dimension_weights = {
            "completeness": 0.35,   # Most critical for handover
            "accuracy": 0.30,       # Critical for operations
            "readiness": 0.20,      # Important for digital transition
            "usefulness": 0.15      # Valuable for facility management
        }
        
        overall_score = 0
        dimension_scores = {}
        
        for dimension, weight in dimension_weights.items():
            if dimension in self.assessment_results:
                score = self.assessment_results[dimension]["score"]
                dimension_scores[dimension] = score
                overall_score += score * weight
        
        # Check mandatory requirements
        mandatory_passed = True
        
        # Must have all mandatory sheets
        mandatory_sheets = self.nima_requirements["mandatory_sheets"]
        available_sheets = list(self.sheets_data.keys())
        missing_mandatory = [sheet for sheet in mandatory_sheets if sheet not in available_sheets]
        
        if missing_mandatory:
            mandatory_passed = False
            print(f"❌ FAILED: Missing mandatory sheets - {missing_mandatory}")
        
        # Must have minimum completeness
        if self.assessment_results.get("completeness", {}).get("score", 0) < 50:
            mandatory_passed = False
            print("❌ FAILED: Completeness below 50%")
        
        # Determine acceptability
        acceptability = "ACCEPTABLE" if (overall_score >= 70 and mandatory_passed) else "NOT ACCEPTABLE"
        
        # Store overall results
        self.assessment_results["overall_score"] = overall_score
        self.assessment_results["acceptability"] = acceptability
        self.assessment_results["dimension_scores"] = dimension_scores
        self.assessment_results["mandatory_passed"] = mandatory_passed
        
        print(f"\n🏆 OVERALL QUALITY SCORE: {overall_score:.1f}%")
        print(f"📋 ACCEPTABILITY: {acceptability}")
        print(f"📊 MANDATORY REQUIREMENTS: {'PASSED' if mandatory_passed else 'FAILED'}")
        
        return overall_score, acceptability
    
    def generate_visual_report(self):
        """Generate visual report with charts"""
        print("\n📈 GENERATING VISUAL REPORT")
        print("=" * 50)
        
        # Create figure with subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Quality Dimensions', 'Acceptance Criteria', 
                          'Sheet Completeness', 'Improvement Priority'),
            specs=[[{'type': 'polar'}, {'type': 'xy'}],
                   [{'type': 'xy'}, {'type': 'domain'}]],
            vertical_spacing=0.15,
            horizontal_spacing=0.15
        )
        
        # 1. Radar chart for dimensions
        dimensions = ['Completeness', 'Accuracy', 'Readiness', 'Usefulness']
        scores = [self.assessment_results.get(dim.lower(), {}).get("score", 0) for dim in dimensions]
        
        fig.add_trace(
            go.Scatterpolar(
                r=scores + [scores[0]],
                theta=dimensions + [dimensions[0]],
                fill='toself',
                fillcolor='rgba(135, 206, 250, 0.3)',
                line=dict(color='rgb(30, 144, 255)', width=2),
                name='COBie Quality'
            ),
            row=1, col=1
        )
        
        # Add threshold circle
        fig.add_trace(
            go.Scatterpolar(
                r=[70, 70, 70, 70, 70],
                theta=dimensions + [dimensions[0]],
                line=dict(color='red', dash='dash', width=1),
                name='Acceptance Threshold'
            ),
            row=1, col=1
        )
        
        # 2. Bar chart for acceptance criteria
        criteria = ['Mandatory Sheets', 'Completeness', 'Accuracy', 'Readiness', 'Usefulness']
        thresholds = [100, 70, 75, 65, 60]  # Threshold percentages
        actual_scores = []
        
        # Get actual scores
        if self.assessment_results.get("file_info", {}).get("sheets_count", 0) > 0:
            mandatory_sheets = self.nima_requirements["mandatory_sheets"]
            available_sheets = list(self.sheets_data.keys())
            sheet_score = sum(1 for sheet in mandatory_sheets if sheet in available_sheets) / len(mandatory_sheets) * 100
            actual_scores.append(sheet_score)
        else:
            actual_scores.append(0)
        
        actual_scores.append(self.assessment_results.get("completeness", {}).get("score", 0))
        actual_scores.append(self.assessment_results.get("accuracy", {}).get("score", 0))
        actual_scores.append(self.assessment_results.get("readiness", {}).get("score", 0))
        actual_scores.append(self.assessment_results.get("usefulness", {}).get("score", 0))
        
        fig.add_trace(
            go.Bar(
                x=criteria,
                y=thresholds,
                name='Threshold',
                marker_color='lightgray',
                opacity=0.5
            ),
            row=1, col=2
        )
        
        fig.add_trace(
            go.Bar(
                x=criteria,
                y=actual_scores,
                name='Actual',
                marker_color=['green' if a >= t else 'red' for a, t in zip(actual_scores, thresholds)]
            ),
            row=1, col=2
        )
        
        # 3. Sheet completeness heatmap
        if self.sheets_data:
            sheet_names = list(self.sheets_data.keys())[:8]  # Top 8 sheets
            completeness_scores = []
            
            for sheet in sheet_names:
                if sheet in self.cobie_schema:
                    required = self.cobie_schema[sheet]["required"]
                    available = self.sheets_data[sheet].columns.tolist()
                    present = [col for col in required if col in available]
                    score = len(present) / len(required) * 100 if required else 0
                    completeness_scores.append(score)
                else:
                    completeness_scores.append(0)
            
            fig.add_trace(
                go.Bar(
                    x=sheet_names,
                    y=completeness_scores,
                    name='Sheet Completeness',
                    marker_color='orange'
                ),
            row=2, col=1
        )
        
        # 4. Improvement priority pie chart
        issues_by_category = {
            'Completeness': len(self.assessment_results.get("completeness", {}).get("findings", [])),
            'Accuracy': len(self.assessment_results.get("accuracy", {}).get("findings", [])),
            'Readiness': len(self.assessment_results.get("readiness", {}).get("findings", [])),
            'Usefulness': len(self.assessment_results.get("usefulness", {}).get("findings", []))
        }
        
        issues_by_category = {k: v for k, v in issues_by_category.items() if v > 0}
        
        if issues_by_category:
            fig.add_trace(
                go.Pie(
                    labels=list(issues_by_category.keys()),
                    values=list(issues_by_category.values()),
                    name='Improvement Areas',
                    hole=0.4,
                    textinfo='label+percent'
                ),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(
            height=800,
            showlegend=True,
            title_text=f"COBie Quality Assessment: {self.file_name}",
            title_x=0.5
        )
        
        fig.update_polars(
            radialaxis=dict(range=[0, 100], tickfont=dict(size=10)),
            angularaxis=dict(tickfont=dict(size=12))
        )
        
        fig.show()
        
        # Additional detailed charts
        self._create_detailed_charts()
    
    def _create_detailed_charts(self):
        """Create additional detailed charts"""
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('Detailed COBie Analysis', fontsize=16, fontweight='bold')
        
        # Chart 1: Data distribution by sheet
        sheet_sizes = {sheet: len(df) for sheet, df in self.sheets_data.items()}
        if sheet_sizes:
            sorted_sheets = dict(sorted(sheet_sizes.items(), key=lambda x: x[1], reverse=True)[:10])
            axes[0, 0].barh(list(sorted_sheets.keys()), list(sorted_sheets.values()), color='skyblue')
            axes[0, 0].set_title('Top 10 Sheets by Row Count')
            axes[0, 0].set_xlabel('Number of Rows')
        
        # Chart 2: Missing data heatmap
        if self.sheets_data:
            sample_sheet = list(self.sheets_data.keys())[0]
            if sample_sheet in self.sheets_data:
                df = self.sheets_data[sample_sheet]
                missing_data = df.isnull().sum()
                if len(missing_data) > 0:
                    axes[0, 1].bar(range(len(missing_data[:10])), list(missing_data[:10]), color='salmon')
                    axes[0, 1].set_title(f'Missing Data in {sample_sheet}')
                    axes[0, 1].set_ylabel('Missing Values')
                    axes[0, 1].set_xticks(range(len(missing_data[:10])))
                    axes[0, 1].set_xticklabels(list(missing_data.index[:10]), rotation=45, ha='right')
        
        # Chart 3: Dimension scores comparison
        dimensions = ['Completeness', 'Accuracy', 'Readiness', 'Usefulness']
        scores = [self.assessment_results.get(dim.lower(), {}).get("score", 0) for dim in dimensions]
        thresholds = [self.quality_thresholds[dim.lower()]["overall"] * 100 for dim in dimensions]
        
        x = np.arange(len(dimensions))
        width = 0.35
        
        axes[1, 0].bar(x - width/2, scores, width, label='Actual', color='lightblue')
        axes[1, 0].bar(x + width/2, thresholds, width, label='Threshold', color='lightgray', alpha=0.7)
        axes[1, 0].set_title('Dimension Scores vs Thresholds')
        axes[1, 0].set_ylabel('Score (%)')
        axes[1, 0].set_xticks(x)
        axes[1, 0].set_xticklabels(dimensions, rotation=45, ha='right')
        axes[1, 0].legend()
        
        # Chart 4: Acceptability gauge
        overall_score = self.assessment_results.get("overall_score", 0)
        acceptability = self.assessment_results.get("acceptability", "UNKNOWN")
        
        axes[1, 1].axis('off')
        color = 'green' if acceptability == "ACCEPTABLE" else 'red'
        
        axes[1, 1].text(0.5, 0.7, f'{overall_score:.1f}%', 
                       ha='center', va='center', fontsize=36, fontweight='bold', color=color)
        axes[1, 1].text(0.5, 0.5, acceptability, 
                       ha='center', va='center', fontsize=24, color=color)
        axes[1, 1].text(0.5, 0.3, f'File: {self.file_name[:20]}...', 
                       ha='center', va='center', fontsize=12)
        
        plt.tight_layout()
        plt.show()
    
    def generate_text_report(self):
        """Generate comprehensive text report"""
        print("\n" + "="*70)
        print("📋 COBie DATA QUALITY ASSESSMENT REPORT")
        print("="*70)
        
        # Header
        print(f"File: {self.file_name}")
        print(f"Assessment Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Standards: ISO19650 & NIMA UK")
        print("-"*70)
        
        # Executive Summary
        print("\n📊 EXECUTIVE SUMMARY")
        print("-"*40)
        
        overall_score = self.assessment_results.get("overall_score", 0)
        acceptability = self.assessment_results.get("acceptability", "UNKNOWN")
        
        print(f"Overall Score: {overall_score:.1f}%")
        print(f"Acceptability: {acceptability}")
        print(f"Mandatory Requirements: {'MET' if self.assessment_results.get('mandatory_passed', False) else 'NOT MET'}")
        
        # Dimension Scores
        print("\n📈 DIMENSION SCORES")
        print("-"*40)
        print(f"{'Dimension':15} {'Score':8} {'Status':12} {'Threshold':10}")
        print("-"*40)
        
        for dim in ['completeness', 'accuracy', 'readiness', 'usefulness']:
            if dim in self.assessment_results:
                data = self.assessment_results[dim]
                score = data.get("score", 0)
                status = data.get("status", "UNKNOWN")
                threshold = self.quality_thresholds[dim]["overall"] * 100
                
                print(f"{dim.capitalize():15} {score:7.1f}%  {status:12} {threshold:9.0f}%")
        
        # Critical Findings
        print("\n🔍 CRITICAL FINDINGS")
        print("-"*40)
        
        all_findings = []
        for dim in ['completeness', 'accuracy', 'readiness', 'usefulness']:
            if dim in self.assessment_results:
                findings = self.assessment_results[dim].get("findings", [])
                all_findings.extend(findings[:3])  # Top 3 from each dimension
        
        if all_findings:
            for i, finding in enumerate(all_findings[:10], 1):
                print(f"{i}. {finding}")
        else:
            print("✅ No critical findings identified")
        
        # Recommendations
        print("\n💡 RECOMMENDATIONS FOR IMPROVEMENT")
        print("-"*40)
        
        all_recommendations = []
        for dim in ['completeness', 'accuracy', 'readiness', 'usefulness']:
            if dim in self.assessment_results:
                recs = self.assessment_results[dim].get("recommendations", [])
                all_recommendations.extend(recs)
        
        if all_recommendations:
            # Remove duplicates
            unique_recs = list(dict.fromkeys(all_recommendations))
            for i, rec in enumerate(unique_recs[:10], 1):
                print(f"{i}. {rec}")
        else:
            print("✅ All requirements met!")
        
        # NIMA UK Compliance
        print("\n🇬🇧 NIMA UK COMPLIANCE CHECK")
        print("-"*40)
        
        mandatory_sheets = self.nima_requirements["mandatory_sheets"]
        available_sheets = list(self.sheets_data.keys())
        
        print("Mandatory Sheets Status:")
        for sheet in mandatory_sheets:
            status = "✓ PRESENT" if sheet in available_sheets else "✗ MISSING"
            print(f"  {sheet:15} {status}")
        
        # ISO19650 Compliance
        print("\n📐 ISO19650 COMPLIANCE CHECK")
        print("-"*40)
        
        print("Information Requirements:")
        for req in self.iso19650_requirements["information_requirements"]:
            print(f"  □ {req}")
        
        # Action Plan
        print("\n🎯 ACTION PLAN")
        print("-"*40)
        
        if acceptability == "NOT ACCEPTABLE":
            print("IMMEDIATE ACTIONS REQUIRED:")
            print("1. Add all missing mandatory sheets")
            print("2. Achieve at least 70% completeness score")
            print("3. Fix critical data accuracy issues")
            print("4. Implement standardized naming conventions")
        else:
            print("MAINTENANCE ACTIONS:")
            print("1. Regular data quality audits")
            print("2. Update warranty and maintenance information")
            print("3. Enhance Digital Twin readiness")
            print("4. Continuous improvement of classification")
        
        print("\n" + "="*70)
        
        # Save report to file
        self._save_report_to_file()
    
    def _save_report_to_file(self):
        """Save assessment report to text file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"COBie_Assessment_Report_{timestamp}.txt"
        
        # Redirect print output to file
        import sys
        original_stdout = sys.stdout
        
        with open(report_filename, 'w') as f:
            sys.stdout = f
            self.generate_text_report()
            sys.stdout = original_stdout
        
        print(f"\n💾 Report saved as: {report_filename}")
        
        # Offer download
        try:
            files.download(report_filename)
        except:
            print("📄 Report saved in Colab workspace")
    
    def generate_improvement_plan(self):
        """Generate detailed improvement plan"""
        print("\n🔄 GENERATING IMPROVEMENT PLAN")
        print("="*70)
        
        plan = {
            "priority": {
                "high": [],
                "medium": [],
                "low": []
            },
            "timeline": {
                "immediate": [],
                "short_term": [],
                "long_term": []
            },
            "responsibilities": {
                "data_manager": [],
                "facility_manager": [],
                "contractor": []
            }
        }
        
        # High Priority Issues
        overall_score = self.assessment_results.get("overall_score", 0)
        if overall_score < 70:
            plan["priority"]["high"].append("Achieve minimum 70% overall score")
        
        mandatory_sheets = self.nima_requirements["mandatory_sheets"]
        available_sheets = list(self.sheets_data.keys())
        missing_sheets = [sheet for sheet in mandatory_sheets if sheet not in available_sheets]
        
        if missing_sheets:
            for sheet in missing_sheets:
                plan["priority"]["high"].append(f"Add {sheet} sheet")
                plan["timeline"]["immediate"].append(f"Create {sheet} sheet with required columns")
                plan["responsibilities"]["data_manager"].append(f"Populate {sheet} data")
        
        # Medium Priority Issues
        completeness_score = self.assessment_results.get("completeness", {}).get("score", 0)
        if completeness_score < 80:
            plan["priority"]["medium"].append("Improve completeness to 80%")
            plan["timeline"]["short_term"].append("Fill missing required columns")
        
        accuracy_score = self.assessment_results.get("accuracy", {}).get("score", 0)
        if accuracy_score < 75:
            plan["priority"]["medium"].append("Fix data accuracy issues")
            plan["timeline"]["short_term"].append("Validate and correct data formats")
        
        # Low Priority Issues
        readiness_score = self.assessment_results.get("readiness", {}).get("score", 0)
        if readiness_score < 65:
            plan["priority"]["low"].append("Enhance Digital Twin readiness")
            plan["timeline"]["long_term"].append("Implement classification system")
        
        usefulness_score = self.assessment_results.get("usefulness", {}).get("score", 0)
        if usefulness_score < 60:
            plan["priority"]["low"].append("Add facility management data")
            plan["timeline"]["long_term"].append("Include warranty and maintenance info")
        
        # Print improvement plan
        print("\n🎯 PRIORITIZED IMPROVEMENT PLAN")
        print("-"*40)
        
        print("🔴 HIGH PRIORITY:")
        for i, item in enumerate(plan["priority"]["high"], 1):
            print(f"  {i}. {item}")
        
        print("\n🟡 MEDIUM PRIORITY:")
        for i, item in enumerate(plan["priority"]["medium"], 1):
            print(f"  {i}. {item}")
        
        print("\n🟢 LOW PRIORITY:")
        for i, item in enumerate(plan["priority"]["low"], 1):
            print(f"  {i}. {item}")
        
        print("\n⏰ TIMELINE:")
        print("Immediate (1-2 days):")
        for item in plan["timeline"]["immediate"]:
            print(f"  • {item}")
        
        print("\nShort-term (1-2 weeks):")
        for item in plan["timeline"]["short_term"]:
            print(f"  • {item}")
        
        print("\nLong-term (1-2 months):")
        for item in plan["timeline"]["long_term"]:
            print(f"  • {item}")
        
        print("\n👥 RESPONSIBILITIES:")
        print("Data Manager:")
        for item in plan["responsibilities"]["data_manager"]:
            print(f"  • {item}")
        
        print("\nFacility Manager:")
        for item in plan["responsibilities"]["facility_manager"]:
            print(f"  • {item}")
        
        return plan
    
    def run_complete_assessment(self):
        """Run complete assessment workflow"""
        print("\n" + "="*70)
        print("🚀 STARTING COMPLETE COBie ASSESSMENT")
        print("="*70)
        
        # Step 1: Upload file
        file_content = self.upload_cobie_file()
        if not file_content:
            return False
        
        # Step 2: Load data
        if not self.load_cobie_data(file_content):
            return False
        
        # Step 3: Run assessments
        print("\n" + "="*70)
        print("🔍 RUNNING ASSESSMENTS")
        print("="*70)
        
        self.assess_completeness()
        self.assess_accuracy()
        self.assess_readiness()
        self.assess_usefulness()
        
        # Step 4: Calculate overall score
        self.calculate_overall_score()
        
        # Step 5: Generate reports
        print("\n" + "="*70)
        print("📊 GENERATING REPORTS")
        print("="*70)
        
        self.generate_visual_report()
        self.generate_text_report()
        
        # Step 6: Improvement plan
        self.generate_improvement_plan()
        
        print("\n" + "="*70)
        print("✅ ASSESSMENT COMPLETE")
        print("="*70)
        
        return True

def batch_assessment():
    """Run assessment on multiple COBie files"""
    print("\n📁 BATCH ASSESSMENT MODE")
    print("="*70)
    
    # Upload multiple files
    uploaded_files = files.upload()
    
    if not uploaded_files:
        print("❌ No files uploaded!")
        return
    
    results = []
    
    for file_name, file_content in uploaded_files.items():
        print(f"\n{'='*60}")
        print(f"📊 Assessing: {file_name}")
        print(f"{'='*60}")
        
        analyzer = COBieQualityAnalyzer()
        analyzer.file_name = file_name
        
        if analyzer.load_cobie_data(file_content):
            analyzer.assess_completeness()
            analyzer.assess_accuracy()
            analyzer.assess_readiness()
            analyzer.assess_usefulness()
            analyzer.calculate_overall_score()
            
            overall_score = analyzer.assessment_results.get("overall_score", 0)
            acceptability = analyzer.assessment_results.get("acceptability", "UNKNOWN")
            
            results.append({
                "file": file_name,
                "score": overall_score,
                "status": acceptability,
                "completeness": analyzer.assessment_results.get("completeness", {}).get("score", 0),
                "accuracy": analyzer.assessment_results.get("accuracy", {}).get("score", 0),
                "readiness": analyzer.assessment_results.get("readiness", {}).get("score", 0),
                "usefulness": analyzer.assessment_results.get("usefulness", {}).get("score", 0)
            })
            
            print(f"\n📋 Results for {file_name}:")
            print(f"  Overall Score: {overall_score:.1f}%")
            print(f"  Acceptability: {acceptability}")
    
    # Display batch summary
    if results:
        print("\n" + "="*70)
        print("📋 BATCH ASSESSMENT SUMMARY")
        print("="*70)
        
        print(f"\n{'File':40} {'Score':6} {'Status':15} {'C':6} {'A':6} {'R':6} {'U':6}")
        print("-"*85)
        
        for result in sorted(results, key=lambda x: x["score"], reverse=True):
            status_icon = "✅" if result["status"] == "ACCEPTABLE" else "❌"
            print(f"{status_icon} {result['file'][:38]:38} {result['score']:5.1f}% {result['status'][:13]:13} "
                  f"{result['completeness']:5.1f}% {result['accuracy']:5.1f}% "
                  f"{result['readiness']:5.1f}% {result['usefulness']:5.1f}%")
        
        # Calculate averages
        avg_score = np.mean([r["score"] for r in results])
        acceptable_count = sum(1 for r in results if r["status"] == "ACCEPTABLE")
        
        print(f"\n📊 Summary:")
        print(f"  Files Assessed: {len(results)}")
        print(f"  Average Score: {avg_score:.1f}%")
        print(f"  Acceptable Files: {acceptable_count}/{len(results)}")
        print(f"  Acceptance Rate: {acceptable_count/len(results)*100:.1f}%")

# Main execution
if __name__ == "__main__":
    print("\n" + "="*70)
    print("🏗️  COBie Data Quality Assessment Tool")
    print("📐 Standards: ISO19650 & NIMA UK")
    print("👷 Industry: Construction & Facility Management")
    print("="*70)
    
    # Create analyzer instance
    analyzer = COBieQualityAnalyzer()
    
    # Run complete assessment
    analyzer.run_complete_assessment()
    
    # Uncomment for batch assessment
    # batch_assessment()
    
    print("\n" + "="*70)
    print("🎉 Assessment Complete!")
    print("="*70)
    print("\n📞 For support or questions:")
    print("  • Review the assessment reports")
    print("  • Check the improvement plan")
    print("  • Consult ISO19650 guidelines")
    print("  • Refer to NIMA UK standards")