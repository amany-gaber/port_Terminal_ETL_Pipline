#!/usr/bin/env python3
"""
Data Verification Script
Checks CSV file quality and schema before running the ETL pipeline
"""

import csv
import sys
import os
from datetime import datetime

CSV_FILE = 'data/Alldataset_port_kpi_.csv'

class DataVerifier:
    def __init__(self):
        self.issues = []
        self.warnings = []
        self.info = []
        
    def check_file_exists(self):
        """Check if CSV file exists"""
        if not os.path.exists(CSV_FILE):
            self.issues.append(f"❌ CSV file not found: {CSV_FILE}")
            return False
        self.info.append(f"✅ CSV file found: {CSV_FILE}")
        return True
    
    def check_file_readable(self):
        """Check if CSV file is readable"""
        try:
            with open(CSV_FILE, 'r', encoding='utf-8') as f:
                f.read(100)
            self.info.append("✅ CSV file is readable")
            return True
        except Exception as e:
            self.issues.append(f"❌ Cannot read CSV file: {e}")
            return False
    
    def check_schema(self):
        """Check CSV schema and columns"""
        required_columns = [
            'Call_Key', 'Call_ID', 'Arvl_Dt', 'Dptr_Dt', 'First_Movement_Dt',
            'Last_Movement_Dt', 'VesselName', 'IMO', 'BerthID', 'port_name',
            'Service_ID', 'Containers_Handled', 'Operation_Time', 'status'
        ]
        
        try:
            with open(CSV_FILE, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                missing = [col for col in required_columns if col not in headers]
                if missing:
                    self.warnings.append(f"⚠️  Missing columns: {missing}")
                else:
                    self.info.append(f"✅ All required columns found ({len(required_columns)})")
                
                self.info.append(f"✅ Total columns: {len(headers)}")
                return len(missing) == 0
        except Exception as e:
            self.issues.append(f"❌ Error checking schema: {e}")
            return False
    
    def check_row_count(self):
        """Check number of rows in CSV"""
        try:
            with open(CSV_FILE, 'r', encoding='utf-8') as f:
                row_count = sum(1 for _ in f) - 1  # Subtract header row
                self.info.append(f"✅ Total rows: {row_count:,}")
                
                if row_count < 100:
                    self.warnings.append(f"⚠️  Small dataset: only {row_count} rows")
                elif row_count > 1000000:
                    self.warnings.append(f"⚠️  Large dataset: {row_count:,} rows may require more memory")
                
                return row_count > 0
        except Exception as e:
            self.issues.append(f"❌ Error counting rows: {e}")
            return False
    
    def check_data_quality(self):
        """Check data quality - null values, duplicates, etc"""
        try:
            with open(CSV_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                if not rows:
                    self.issues.append("❌ No data rows found")
                    return False
                
                # Check for null values in key columns
                key_columns = ['Call_Key', 'Call_ID', 'VesselName']
                null_counts = {col: 0 for col in key_columns}
                
                for row in rows:
                    for col in key_columns:
                        if not row.get(col) or row.get(col).strip() == '':
                            null_counts[col] += 1
                
                for col, count in null_counts.items():
                    if count > 0:
                        pct = (count / len(rows)) * 100
                        self.warnings.append(f"⚠️  Column '{col}' has {count} null values ({pct:.2f}%)")
                
                # Check for duplicate Call_Keys
                call_keys = [row.get('Call_Key') for row in rows]
                duplicates = len(call_keys) - len(set(call_keys))
                if duplicates > 0:
                    self.warnings.append(f"⚠️  {duplicates} duplicate Call_Key values found")
                else:
                    self.info.append(f"✅ No duplicate Call_Key values")
                
                self.info.append(f"✅ Data quality check passed")
                return True
        except Exception as e:
            self.issues.append(f"❌ Error checking data quality: {e}")
            return False
    
    def check_file_size(self):
        """Check file size"""
        try:
            size_bytes = os.path.getsize(CSV_FILE)
            size_mb = size_bytes / (1024 * 1024)
            self.info.append(f"✅ File size: {size_mb:.2f} MB")
            
            if size_mb > 2000:
                self.warnings.append("⚠️  Large file size may require more memory")
            
            return True
        except Exception as e:
            self.issues.append(f"❌ Error checking file size: {e}")
            return False
    
    def print_report(self):
        """Print verification report"""
        print("\n" + "=" * 60)
        print("  CSV DATA VERIFICATION REPORT")
        print("=" * 60 + "\n")
        
        print(f"Checked at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        if self.info:
            print("📊 Information:")
            for msg in self.info:
                print(f"  {msg}")
            print()
        
        if self.warnings:
            print("⚠️  Warnings:")
            for msg in self.warnings:
                print(f"  {msg}")
            print()
        
        if self.issues:
            print("❌ Issues:")
            for msg in self.issues:
                print(f"  {msg}")
            print()
        
        print("=" * 60)
        
        if not self.issues:
            print("✅ CSV file is ready for pipeline execution!")
            return True
        else:
            print("❌ Please fix the issues before running the pipeline")
            return False
    
    def run(self):
        """Run all verification checks"""
        checks = [
            ("File Existence", self.check_file_exists),
            ("File Readable", self.check_file_readable),
            ("Schema", self.check_schema),
            ("Row Count", self.check_row_count),
            ("File Size", self.check_file_size),
            ("Data Quality", self.check_data_quality)
        ]
        
        print("\n🔍 Running verification checks...\n")
        
        for check_name, check_func in checks:
            try:
                print(f"Checking {check_name}...", end=" ")
                result = check_func()
                print("✅" if result else "⚠️ ")
            except Exception as e:
                print(f"❌ {e}")
        
        print()
        success = self.print_report()
        return success

if __name__ == "__main__":
    verifier = DataVerifier()
    success = verifier.run()
    sys.exit(0 if success else 1)
