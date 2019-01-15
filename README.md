# Spark-Taxi-Rides-in-NY
This project is designed to give you an opportunity to gain experience in programming systems in the Hadoop ecosystem. In this case, we use Spark to analyze taxi rides within New York.

We will use a data set which covers one month. You will nd time and location for each
trip's start and end. In the following, this is the data that is meant when we refer to a
trip.
The general question is: Can we match trips and return trips? For a given trip a, we
consider another trip b as a return trip i
1. b's pickup time is within 8 hours after a's dropo time
2. b's pickup location is within r meters of a's dropo location
3. b's dropo location is within r meters of a's pickup location
where r is a distance in meters between 50 and 200.
To compute the return trips, you may want to break the problem down into the following
series of problems:
1. Given the (lat,lon) coordinates
• a(40:79670715332031;􀀀73:97093963623047)
• b(40:789649963378906;􀀀73:94803619384766)
• c(40:73122024536133;􀀀73:9823226928711)
which trips have dropo locations within r meters of a,b or c?
2. For each trip a in the dataset, compute the trips that have a pickup location within
r meters of a's dropo location. These are the return trip candidates.
3. For all trips a in the dataset, compute all trips that may have been return trips for
a.
Another way to characterize the dataset to be returned would be this pseudo SQL:
select *
from
tripsProvided a,
tripsProvided b
where
distance (a. dropofflocation , b. pickuplocation ) < r and
distance (b. dropofflocation , a. pickuplocation ) < r and
a. dropofftime < b. pickuptime and
a. dropofftime + 8 hours > b. pickuptime

For distance calculations, assume that the earth is a sphere with radius 6371km. Numerical
stability of appropriate formulas is discussed e.g. at https://en.wikipedia.org/wiki/Great-
circle distance.
