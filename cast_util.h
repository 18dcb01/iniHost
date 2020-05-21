#ifndef DAVOS_SRC_INCLUDE_DAVOS_UTIL_CAST_UTIL_H_
#define DAVOS_SRC_INCLUDE_DAVOS_UTIL_CAST_UTIL_H_

#include <arrow/compute/api.h>
#include <arrow/status.h>
#include <arrow/util/parsing.h>

#include <boost/algorithm/string.hpp>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "davos/core/storage/memory_manager.h"
#include "davos/util/macros.h"

namespace davos {

namespace util {

enum unit{year, month, day, hour, minute, second, millisecond, ampm, timezone, week, weekday, yearday, unknown = -1};

std::string left_pad(std::string str, int width, std::string pad = "0"){
    std::string result = str;
    while(result.length()<width)
        result = pad+result;
    return result;
}

arrow::Status CastStringToTimestamp(std::shared_ptr<arrow::ChunkedArray> values, arrow::compute::Datum *output) {
    DAVOS_ASSERT_CONDITION(values->type()->id() == arrow::Type::STRING)

    auto timestamp_type = ArrowTypeTraits<arrow::TimestampType>::DataType();
    arrow::TimestampBuilder builder(timestamp_type, MemoryManager::stream_memory_pool());
    arrow::internal::StringConverter<arrow::TimestampType> converter(timestamp_type);
    arrow::Status status;
    for (const auto &array : values->chunks()) {
        if (array->length() == 0) {
            continue;
        }

        auto string_array = std::dynamic_pointer_cast<arrow::StringArray>(array);
        DAVOS_ASSERT_CONDITION(string_array)
        for (int64_t i = 0; i < string_array->length(); ++i) {
            if (!string_array->IsValid(i) || string_array->GetString(i).empty()) {
                status = builder.AppendNull();
            } else {
                auto value = boost::to_lower(string_array->GetString(i));
                std::vector<unit> type;
                std::vector<std::string> tokens;
                boost::split(tokens, value, !boost::is_alnum(), boost::token_compress_on);
                std::map<std::string,std::string> month_dict = {
                    {"jan", "01"}, {"feb", "02"}, {"mar", "03"}, {"apr", "04"},
                    {"may", "05"}, {"jun", "06"}, {"jul", "07"}, {"aug", "08"},
                    {"sep". "09"}, {"oct", "10"}, {"nov", "11"}, {"dec", "12"},
                    {"january", "01"}, {"february", "02"}, {"march", "03"}, {"april", "04"},
                    {"june", "06"}, {"july", "07"}, {"august", "08"},
                    {"september", "09"}, {"october", "10"}, {"november", "11"}, {"december", "12"}};
                
                std::map<std::string,std::string> timezone_dict = {
                    {"est", "-0500"}, {"edt", "-0400"}, {"cst", "-0600"}, {"cdt", "-0500"},
                    {"mst", "-0700"}, {"mdt", "-0600"}, {"pst", "-0800"}, {"pdt", "-0700"},
                    {"a", "+0100"}, {"b", "+0200"}, {"c", "+0300"}, {"d", "+0400"}, {"e", "+0500"},
                    {"f", "+0600"}, {"g", "+0700"}, {"h", "+0800"}, {"i", "+0900"}, {"k", "+1000"},
                    {"l", "+1100"}, {"m", "+1200"}, {"n", "-100"}, {"o", "-0200"}, {"p", "-0300"},
                    {"q", "-0400"}, {"r", "-0500"}, {"s", "-0600"}, {"t", "-0700"}, {"u", "-0800"},
                    {"v", "-0900"}, {"w", "-1000"}, {"x", "-1100"}, {"y", "-1200"}, {"z", ""}
                    {"ut", ""}, {"gmt", ""}, {"utc", ""}};
                /* The code below consists of three sections:
                  --split the datetime into tokens
                  --figure out what those tokens represent
                  --turn those tokens into an ISO-8601 compliant string
                */
                //for loop is the wrong method here. TODO recursively
		for(int i = 0; i<tokens.size(); i++){
                    if(i != type.size())
                        return arrow::Status::UnknownError("Bug in token reparsing");//Should not ever happen
                    if(isdigit(tokens.at(i)[0])){
                        std::size_t firstLetter = tokens.at(i).find_first_not_of("1234567890");
                        type curType = unkown;
                        if(firstLetter != std::string::npos){//string starts with a number, but has letters
                            std::string prefix = tokens.at(i).substr(0,firstLetter);
                            std::string suffix = tokens.at(i).substr(firstLetter);

                            //ordinal days (1st, 2nd, etc)
                            if(prefix.length()<=2 && (suffix == "st" || suffix == "nd" || suffix == "rd" || suffix == "th")){
                                tokens.at(i) = prefix;
                                curType = day;
                            }

                            //am, pm, or timezones
                            else if(suffix == "pm" || suffix == "am" || timezone_dict.find(suffix)!=timezone_dict.end()){
                                tokens.at(i) = prefix;
                                tokens.insert(tokens.begin()+i+1,suffix);
                            }

                            //DDTHH from iso-8601
                            else if(suffix[0] == "t" && suffix.find_first_of("0123456789")==1){
                                tokens.at(i) = prefix;
                                tokens.insert(tokens.begin()+i+1,suffix.substr(1));
                                //could set type to days here, don't have to, design choice
                            }

                            //no space before month (eg 30mar2010 or 15april2015)
                            else if(suffix.length()>=3 && month_dict.find(suffix.substr(0,3))!=month_dict.end()){
                                tokens.at(i) = prefix;
                                tokens.insert(tokens.begin()+i+1, suffix);
                            }

                            else{
                                return arrow::Status::Invalid("Cannot parse token: ", tokens.at(i));
                            }
			}
	                int length = tokens.at(i).length();
                        if(length>2){
                            if(length<=4 && i < 3)
                                curType = year;

                            //year and month concatenated
                            else if(length == 6 && i == 0){
                                tokens.insert(tokens.begin()+i+1,tokens.at(i).substr(4));
                                tokens.at(i) = tokens.at(i).substr(0,4);
                                curType = year;
                            }

                            //year month day concatenated
                            else if(length == 8 && i == 0){
                                tokens.insert(tokens.begin()+i+1, tokens.at(i).substr(4,2));
                                tokens.insert(tokens.begin()+i+2, tokens.at(i).substr(6));
                                tokens.at(i) = tokens.at(i).substr(0,4);
                                curType = year;
                            }
 
                            //hour and minute concatenated
                            else if(length == 4 && i == 3){
                                tokens.insert(tokens.begin()+i+1, tokens.at(i).substr(2));
                                tokens.at(i) = tokens.at(i).substr(0,2);
                                curType = hour;
                            }

                            //hour minute and second concatenated
                            else if(length == 6 && i == 3){
                                tokens.insert(tokens.begin()+i+1, tokens.at(i).substr(2,2));
                                tokens.insert(tokens.begin()+i+2, tokens.at(i).substr(4,2));
                                tokens.at(i) = tokens.at(i).substr(0,2);
                                curType = hour;
                            }

                            //timezone
                            else if(length == 4 && i>3 && i == tokens.size()-1)
                                curType = timezone;

                            else if(length == 3 && i==millisecond)
                                curType = millisecond;
 
                            else
                                return arrow::Status::Invalid("Cannot parse token: ", tokens.at(i));
                        }
                        
                        //at this point, the token must be a 1-2 digit number, or a particular exception(year, timezone)
                        type.append(curType);
                    }
                    else{ //if the token starts with a letter
                        std::size_t firstNumber = tokens.at(i).find_first_of("1234567890")
                        if(firstNumber != std::string::npos){
                            if(firstNumber == 1 && tokens.at(i).at(0)=="t")
                                tokens.at(i) = tokens.at(i).substr(1);
                            else if(month_dict.find(tokens.at(i).substr(0,firstNumber)) != month_dict.end()){
                                tokens.insert(tokens.begin()+i+1, token.at(i).substr(firstNumber));
                                tokens.at(i) = tokens.at(i).substr(0,firstNumber);
                            }
                            else
                                return arrow::Status::Invalid("Cannot parse token: ", tokens.at(i));
                        }

                        //if the token is a day, ignore it
                        if(tokens.at(i).length()>=3){
                            std::string dayCode = token.at(i).substr(0,3);
                            if(dayCode == "mon" || dayCode == "tue" || dayCode == "wed" || dayCode == "thu" || dayCode == "fri" || dayCode == "sat" || dayCode == "sun")
                                tokens.erase(tokens.begin()+i);
                        }    
                        

                        if(isalpha(tokens.at(i).at(0))){
                            if(month_dict.find(tokens.at(i)) != month_dict.end())
                                type.append(month);
                            else if(timezone_dict.find(tokens.at(i)) != timezone_dict.end())
                                type.append(timezone);
                            else if(tokens.at(i) == "am" || tokens.at(i) == "pm" || tokens.at(i) == "a" || tokens.at(i) == "p")
                                type.append(ampm); 
                            else 
                                return arrow::Status::Invalid("Cannot parse token: ", tokens.at(i));
                        }

                        else
                            i--;
                    }
                }

                if(tokens.size() != type.size())
                    return arrow::Status::UnkownError("Bug late in token reparsing");

                //this is where the code starts figuring out what the tokens represent
                //evaluate year-month-day order
                if(tokens.size() >= 3){
                    bool[3][3] possible;
                    for(int i = 0; i<3; i++){
                        for(unit j = year; unit <= day; unit++)
                           possible[i][j] = type.at(i) == j || type.at(i) == unknown;
                        try {
                            int val = boost::lexical_cast<int>(tokens.at(i));
                            if(val>12){
                                possible[i][month] = false;
                                if(val > 31)
                                    possible[i][day] = false;
                            }
                        catch(boost::bad_lexical_cast &) {}
                    

                    if(possible[0][month] && possible[1][day] && possible[2][year]){
                        type.at(0) = month;
                        type.at(1) = day;
                        type.at(2) = year;
                    }
                    else if(possible[0][day] && possible[1][month] && possible[2][year]){
                        type.at(0) = day;
                        type.at(1) = month;
                        type.at(2) = year;
                    }
                    else if(possible[0][year] && possible[1][month] && possible[2][day]){
                        type.at(0) = year;
                        type.at(1) = month;
                        type.at(2) = day;
                    }
                    else
                        return arrow::Status::Invalid("No valid day-month-year ordering: ", value);
                }
                else if(tokens.size() == 2){
                    if(type.at(0)>month||type.at(1)>month||(type.at(0)==type.at(1)&&type.at(0)!=unknown))
                        return arrow::Status::Invalid("No valid month-year ordering: ",value);
                    if(type.at(0) == month || tokens[1] == year){
                        type.at(0) = month;
                        type.at(1) = year;
                    }
                    else{
                        type.at(0) = year;
                        type.at(1) = month;
                    }
                }
                else if(tokens.size() == 1){
                    if(type.at(0)>year)
                        return arrow::Status::Invalid("Single-token date must be year: ", value);
                    type.at(0) = year;
                }
                else
                    return arrow::Status::Invalid("No meaningful tokens in: ", value);
 
                for(unit i = hour; i<= second && i<type.length()){
                    if(type.at(i) == unknown)
                        type.at(i) = i;
                    if(type.at(i) != i)
                        return arrow::Status::Invalid("Token \"", tokens.at(i), "\" has wrong unit type for position ", i);
                }

                for(int i = millisecond; i<type.length(); i++){
                    if(type.at(i) == unknown)
                        type.at(i) = timezone;
                    if(type.at(i) <= type.at(i-1))
                       if(not(type.at(i) == timezone && type.at(i-1) == timezone && type.at(i-2) < timezone)||timezone_dict[tokens.at(i-1)]!="")
                           return arrow::Status::Invalid("Late tokens in incorrect order: ", value);
                }

                //this is where the code parses the tokens
                std::string result[9] = ["No Year Found","01","01","00","00","00","000","",""]

                for(int i = 0; i<type.size(); i++){
                    switch(type.at(i)){
                    case year:
                        int yearVal = boost::lexical_cast<int>(tokens.at(i));//will throw error if I made a mistake somewhere
                        if(tokens.at(i).length()==3)
                            yearVal += 1900;
                        else if(tokens.at(i).length()<=2){
                            if(yearVal >=50)
                                yearVal+=1900;
                            else
                                yearVal+=2000;
                        }
                        result[year] = left_pad(std::to_string(yearVal),4);
                        break;
                    case month:
                        if(month_dict.find(tokens.at(i)) != month_dict.end())
                            result[month] = month_dict[tokens.at(i)];
                        else
                            result[month] = left_pad(tokens.at(i),2);
                        break;
                    case day:
                    case hour:
                    case minute:
                    case second:
                        result[type.at(i)] = left_pad(tokens.at(i),2);
                        break;
                    case millisecond:
                        result[millisecond] = left_pad(tokens.at(i),3);
                    case ampm:
                        if(result[hour]=="12")
                            result[hour] = "00";
                        if(tokens.at(i).at(0)=="p")
                            result[hour] = left_pad(std::to_string(boost::lexical_cast<int>(tokens.at(i))+12),2);
                        break;
                    case timezone:
                        //Note: this can be called multiple times (eg UTC+0700 or Z-0500)
                        if(timezone_dict.find(tokens.at(i)) != timezone_dict.end());
                            result[timezone] = timezone_dict[tokens.at(i)];
                        else{
                            std::string zone = tokens.at(i);
                            if(zone.length()<3):
                                zone = zone+"00"
                            result[timezone] = value.at(value.find_last_of("-+"))+left_pad(zone,4);
                        }
                        break;
                    default:
                        return arrow::Status::UnkownError("Unknown unit: ", tokens.at(i));
                    }
                }   
                
                std::string isoCode = internal::StringBuilder(result[year], "-", result[month], "-", result[day], "T", result[hour], ":", result[minute], ":", result[second], ".", result[millisecond],result[timezone]);
                       
                
                /*    
                // TODO(zeyuan): fix this hack
                if (value.length() > 19) {  // 2019-04-02T09:45:12-0700
                    value = value.substr(0, 19);
                } else if (value.length() <= 9) {  // 1/4/94 or 1-Jan-16
                    std::vector<std::string> tokens;
                    boost::split(tokens, value, boost::is_any_of("-/"));
                    if (tokens.size() == 3) {
                        std::string month, day;
                        if (tokens[1].length() == 3) {  // Jan
                            month = tokens[1], day = tokens[0];
                            std::map<std::string, std::string> month_dict = {
                                {"Jan", "01"}, {"Feb", "02"}, {"Mar", "03"}, {"Apr", "04"},
                                {"May", "05"}, {"Jun", "06"}, {"Jul", "07"}, {"Aug", "08"},
                                {"Sep", "09"}, {"Oct", "10"}, {"Nov", "11"}, {"Dec", "12"}};
                            if (month_dict.find(month) != month_dict.end()) {
                                month = month_dict[month];
                            }
                        } else {  // 4
                            month = tokens[0], day = tokens[1];
                            if (month.length() == 1) {
                                month = "0" + tokens[0];
                            }
                        }
                        std::string year = tokens[2];
                        if (std::stoi(year) <= 25) {
                            year = "20" + year;
                        } else {
                            year = "19" + year;
                        }
                        if (day.length() == 1) {
                            day = "0" + day;
                        }
                        value = internal::StringBuilder(year, "-", month, "-", day);
                    }
                }*/
                ArrowTypeTraits<arrow::TimestampType>::ValueType timestamp_value;
                if (!converter(isoCode.c_str(), isoCode.length(), &timestamp_value)) {
                    return arrow::Status::TypeError("Cannot convert isoCode to timestamp: ", isoCode);
                }
                status = builder.Append(timestamp_value);
            }
            if (!status.ok()) {
                return status;
            }
        }
    }

    std::shared_ptr<arrow::Array> output_array;
    status = builder.Finish(&output_array);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<arrow::ChunkedArray> output_chunked_array = std::make_shared<arrow::ChunkedArray>(output_array);
    output->value = output_chunked_array;
    return arrow::Status::OK();
}

}  // namespace util
}  // namespace davos

#endif  // DAVOS_SRC_INCLUDE_DAVOS_UTIL_CAST_UTIL_H_
