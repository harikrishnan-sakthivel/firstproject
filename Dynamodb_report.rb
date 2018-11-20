# The object of this code is to find the under utilised and utilsed DynamoDB tables in all our AWS account.
# Points to be  noted:
#	1.Listing DynamoDb tables in region wise in AWS account
#	2.we will describe the each table and get the metrics like provisioned read,write limits and Item count
#	3.Based on the consumption limit, we will filter the tables
#	4. Time frame: 
#		one hour = 3600 seconds
#		two weeks = 1209600 seconds 
#	5.lower = minimum consumption level
#	6.Higher = Maximum consumption level
#	7. If the table consumed lower than 1/3 th of its prvisioned value then its called under utilised table
 
require 'net/smtp'
task :dynamodb_report => :environment do
	CURRENT_TIME = Time.new
	provision =Array.new
	consumption = Array.new
	alltable=Array.new
	final_result=Array.new
	consumption_limit=Array.new
	consumedTablesList=Array.new
	accMaster = AccountMaster.where('del_flg = ?', "N")
	accMaster.each do |acc_name|
		if acc_name.alias_name == "freshdesk-staging"
			region = Aws::EC2::Client.new(region: "ap-south-1")
			ls_region = region.describe_regions()
			ls_region.regions.each do |reg|
				cw = Aws::CloudWatch::Client.new(region:"#{reg.region_name}")
		    	db = Aws::DynamoDB::Client.new(region:"#{reg.region_name}")
		    	new_db=Aws::DynamoDB::Resource.new(region: "#{reg.region_name}")
		    	pro_list=new_db.tables.each do |table_name|
		    		desc_tabe = db.describe_table({table_name: "#{table_name.name}"})
				alltable.push("#{acc_name.alias_name}|#{reg.region_name}|#{table_name.name}|#{desc_tabe.table.provisioned_throughput.read_capacity_units}|#{desc_tabe.table.provisioned_throughput.write_capacity_units}|#{desc_tabe.table.item_count}")
				dytable=File.new('/home/deploy/dynamodb-task/Dynamodb_alltables.csv','w')
				dytable.write("Account name  |\tRegion |\tTable name|\tProvisioned read capacity |\tprovisioned write capacity units|\tItem count\n")
				dytable.write("#{alltable.join("\t\n")}")				
				dytable.close
				data = cw.get_metric_statistics({namespace: "AWS/DynamoDB", metric_name: "ConsumedWriteCapacityUnits", dimensions: [{name: "TableName", value: "#{table_name.name}",},],start_time: CURRENT_TIME - 1209600,end_time: CURRENT_TIME, period: 3600, statistics: ["Maximum"],unit: "Count",  })
				 lower=desc_tabe.table.provisioned_throughput.write_capacity_units * 0.01
                                higher=desc_tabe.table.provisioned_throughput.write_capacity_units * 0.33
				sum=0
				data.datapoints.each do |temp|
                                	sum = sum + temp.maximum
				end
                                metrics_average=(sum / 336) # 1 day = 24 hours, 2 weeks = 336 hours
				data.datapoints.each do |temp|
				if "#{metrics_average}".between?("#{lower}", "#{higher}")
						consumption.push("#{acc_name.alias_name}|#{reg.region_name}|#{table_name.name}|#{desc_tabe.table.provisioned_throughput.read_capacity_units} | #{desc_tabe.table.provisioned_throughput.write_capacity_units}  | #{desc_tabe.table.item_count} |#{sum} |  #{metrics_average}")
						
						f=File.new('/home/deploy/dynamodb-task/Dynamodb_consumedcapacityTable.csv','w')
						f.write("Account Name\t| Region  | Table name   | \tProvisioned read capacity |\tprovisioned write capacity units\t|   Item count |\t Sum of consumed write capacity units | \t value per hour\n")
						consumption_uniq=consumption.uniq # finding uniq values in the list
					        
						f.write("#{consumption_uniq.join("\t\n")}") # writing values and metrics into file
						f.close
						consumption_limit.push("#{acc_name.alias_name}|#{reg.region_name}|#{table_name.name}|#{desc_tabe.table.provisioned_throughput.read_capacity_units}|#{desc_tabe.table.provisioned_throughput.write_capacity_units}|#{desc_tabe.table.item_count}")
                                                new_file=File.new('/home/deploy/dynamodb-task/Dynamodb_UnderUtilised.csv','w')
                                                cons_uniq=consumption_limit.uniq # finding uniq values in the list
                                                new_file.write("#{cons_uniq.join("\t\n")}")
                                                new_file.close
			
				elsif "#{metrics_average}" > "#{higher}" 

				                consumedTablesList.push("#{acc_name.alias_name}|#{reg.region_name}|#{table_name.name}|#{desc_tabe.table.provisioned_throughput.read_capacity_units}|#{desc_tabe.table.provisioned_throughput.write_capacity_units}|#{desc_tabe.table.item_count}")
                                                consume=File.new('/home/deploy/dynamodb-task/Dynamodb_utilised.csv','w')
                                                con_tables=consumedTablesList.uniq  # finding uniq values in the list
						consume.write("#{con_tables.join("\t\n")}") # writing values and metrics into file
                                                consume.close
										
					end
				end
			end
			
end	
		else
			region = Aws::EC2::Client.new(region: "ap-south-1")
			ls_region = region.describe_regions()
			ls_region.regions.each do |reg|
				provider = Aws::SharedCredentials.new(profile_name: acc_name.alias_name)
				cw = Aws::CloudWatch::Client.new(credentials: provider,region:"#{reg.region_name}")
		    	db = Aws::DynamoDB::Client.new(credentials: provider,region:"#{reg.region_name}")
		    	new_db=Aws::DynamoDB::Resource.new(credentials: provider,region: "#{reg.region_name}")
		    	pro_list=new_db.tables.each do |table_name|
		    		desc_tabe = db.describe_table({table_name: "#{table_name.name}"})
				alltable.push("#{acc_name.alias_name}|#{reg.region_name}|#{table_name.name}|#{desc_tabe.table.provisioned_throughput.read_capacity_units}|#{desc_tabe.table.provisioned_throughput.write_capacity_units}|#{desc_tabe.table.item_count}")
                                dytable=File.new('/home/deploy/dynamodb-task/Dynamodb_alltables.csv','w')                              
                                dytable.write("Account name  |\tRegion |\tTable name|\tProvisioned read capacity |\tprovisioned write capacity units|\tItem count\n")
				dytable.write("#{alltable.join("\t\n")}")                            
                                dytable.close
				data = cw.get_metric_statistics({namespace: "AWS/DynamoDB", metric_name: "ConsumedWriteCapacityUnits", dimensions: [{name: "TableName", value: "#{table_name.name}",},],start_time: CURRENT_TIME - 1209600,end_time: CURRENT_TIME, period: 3600, statistics: ["Maximum"],unit: "Count",  })
				lower=desc_tabe.table.provisioned_throughput.write_capacity_units * 0.01
                                higher=desc_tabe.table.provisioned_throughput.write_capacity_units * 0.33 
				sum=0
				data.datapoints.each do |temp|
                                	sum = sum + temp.maximum
				end
                                metrics_average=(sum / 336) # 1 day = 24 hours, 2 weeks = 336 hours
				data.datapoints.each do |temp|
                                	if "#{metrics_average}".between?("#{lower}","#{higher}") then
							consumption.push("#{acc_name.alias_name}|#{reg.region_name}|#{table_name.name}|#{desc_tabe.table.provisioned_throughput.read_capacity_units} | #{desc_tabe.table.provisioned_throughput.write_capacity_units}  | #{desc_tabe.table.item_count} |#{sum} |  #{metrics_average}")
						f=File.new('/home/deploy/dynamodb-task/Dynamodb_consumedcapacityTable.csv','w')
						f.write("Account Name\t| Region  | Table name   | \tProvisioned read capacity |provisioned write capacity units\t|   Item count |\t Sum of consumed write capacity units| \t value per hour\n")
						consumption_uniq=consumption.uniq # finding uniq values in the list
						f.write("#{consumption_uniq.join("\t\n")}")
						f.close
						consumption_limit.push("#{acc_name.alias_name}|#{reg.region_name}|#{table_name.name}|#{desc_tabe.table.provisioned_throughput.read_capacity_units}|#{desc_tabe.table.provisioned_throughput.write_capacity_units}|#{desc_tabe.table.item_count}")
						new_file=File.new('/home/deploy/dynamodb-task/Dynamodb_UnderUtilised.csv','w')
						cons_uniq=consumption_limit.uniq # finding uniq values in the list
						new_file.write("#{cons_uniq.join("\t\n")}")
						new_file.close
					elsif "#{metrics_average}" > "#{higher}" 	
						consumedTablesList.push("#{acc_name.alias_name}|#{reg.region_name}|#{table_name.name}|#{desc_tabe.table.provisioned_throughput.read_capacity_units}|#{desc_tabe.table.provisioned_throughput.write_capacity_units}|#{desc_tabe.table.item_count}")
						
						consume=File.new('/home/deploy/dynamodb-task/Dynamodb_utilised.csv','w')
						con_tables=consumedTablesList.uniq # finding uniq values in the list
						consume.write("#{con_tables.join("\t\n")}")
						consume.close
					end			
		
				end
				end
end
end	
end
end

# The object of this code is to find the under utilised and utilsed DynamoDB tables in all our AWS account.
# Points to be  noted:
#       1.Listing DynamoDb tables in region wise in AWS account
#       2.we will describe the each table and get the metrics like provisioned read,write limits and Item count
#       3.Based on the consumption limit, we will filter the tables
#       4. Time frame: 
#               one hour = 3600 seconds
#               two weeks = 1209600 seconds 
#       5.lower = minimum consumption level
#       6.Higher = Maximum consumption level
#       7. If the table consumed lower than 1/3 th of its prvisioned value then its called under utilised table