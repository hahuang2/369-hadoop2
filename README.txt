Anthony Huang

Part 1:

For this part I used three MapReduce programs: HostCountryJoin, Sum, and SortDes. The HostCountryJoin program combines information from the access log and the hostname_country.csv file using the hostname as the key. This allows it to match each web request to the country it came from. If a hostname is not found in the country file, the program marks it as “Unknown Location.” The output from this phase lists each country and the number of requests associated with it. The Sum program then reads this output and adds up all the counts for each country to get a total number of web requests per country. Finally, the SortDes program sorts the countries in descending order by their total request count so that the country with the highest number of requests appears first.

I chose to use a reduce-side join because it works better for large or uneven data. It makes sure that all matching hostnames from both files go to the same reducer, so every record can be correctly joined. This method is slower than a map-side join but more reliable and easier to use for this kind of dataset.

Part2:
For this part I used three MapReduce programs: HostCountryUrlJoin, CountryUrlCount, and SortCountryAZ. The HostCountryUrlJoin program combines data from the access log and the hostname_country.csv file using the hostname as the key. This join step matches each web request to the country of origin, creating pairs of countries and URLs visited by users from that country. The CountryUrlCount program then reads the joined data and counts how many times each (country, URL) pair appears, giving the total number of visits per country for each specific page. Finally, the SortCountryAZ program sorts the output alphabetically by country name and then by count in descending order, showing which URLs are most frequently visited in each country in a clear and organized format.


Part 3:
For this part I used two MapReduce programs: UrlCountryList and UrlCountryAgg. The UrlCountryList program combines information from the access log and the hostname_country.csv file using the hostname as the key, allowing it to match each web request to the correct country. It then outputs each URL along with the country of the visitor. The UrlCountryAgg program reads this output and groups together all countries that visited the same URL. It removes duplicates so that each country is listed only once, then sorts the country names alphabetically and joins them into a single comma-separated list for each URL. The final report lists URLs in alphabetical order along with the unique set of countries that accessed each page, making it easy to see the geographic diversity of web traffic.
