CREATE TABLE IF NOT EXISTS public.business
(
    id     VARCHAR(1024) PRIMARY KEY,
    address  VARCHAR(1024),
    categories VARCHAR(1024),
    city VARCHAR(1024),
    is_open SMALLINT,
    latitude DECIMAL(19,16),
    longitude DECIMAL(19,16),
    name VARCHAR(1024),
    postal_code VARCHAR(1024),
    review_count SMALLINT,
    stars DECIMAL(3,2),
    state VARCHAR(1024)
);

CREATE TABLE IF NOT EXISTS public.user
(
    id     VARCHAR(1024) PRIMARY KEY,
    name VARCHAR(1024),
    review_count INTEGER,
    yelping_since TIMESTAMP,
    average_stars DECIMAL(4,3),
    cool INTEGER,
    fans INTEGER,
    funny INTEGER,
    useful INTEGER,
    elite VARCHAR,
    friends VARCHAR,
    compliment_cool INTEGER,
    compliment_cute INTEGER,
    compliment_funny INTEGER,
    compliment_hot INTEGER,
    compliment_list INTEGER,
    compliment_more INTEGER,
    compliment_note INTEGER,
    compliment_photos INTEGER,
    compliment_plain INTEGER,
    compliment_profile INTEGER,
    compliment_writer INTEGER
);

CREATE TABLE IF NOT EXISTS public.checkin
(
    id     SERIAL PRIMARY KEY,
    business_id VARCHAR(1024),
    date VARCHAR,
    FOREIGN KEY (business_id) REFERENCES public.business(id)
);

CREATE TABLE IF NOT EXISTS public.hours
(
    id     SERIAL PRIMARY KEY,
    business_id VARCHAR(1024),
    Monday VARCHAR(1024),
    Tuesday VARCHAR(1024),
    Wednesday VARCHAR(1024),
    Thursday VARCHAR(1024),
    Friday VARCHAR(1024),
    Saturday VARCHAR(1024),
    Sunday VARCHAR(1024),
    FOREIGN KEY (business_id) REFERENCES public.business(id)
);

CREATE TABLE IF NOT EXISTS public.tip
(
    id     SERIAL PRIMARY KEY,
    business_id VARCHAR(1024),
    user_id VARCHAR(1024),
    date TIMESTAMP,
    compliment_count INTEGER,
    text TEXT,
    FOREIGN KEY (business_id) REFERENCES public.business(id),
    FOREIGN KEY (user_id) REFERENCES public.user(id)
);

CREATE TABLE IF NOT EXISTS public.review
(
    id     VARCHAR(1024) PRIMARY KEY,
    business_id VARCHAR(1024),
    user_id VARCHAR(1024),
    date TIMESTAMP,
    cool INTEGER,
    funny INTEGER,
    stars DECIMAL(3,2),
    useful INTEGER,
    date_string VARCHAR(1024),
    text TEXT,
    FOREIGN KEY (business_id) REFERENCES public.business(id),
    FOREIGN KEY (user_id) REFERENCES public.user(id)
);

CREATE TABLE IF NOT EXISTS public.weather_temperature
(
    id     SERIAL PRIMARY KEY,
    date VARCHAR(1024),
    min INTEGER,
    max INTEGER,
    normal_min DECIMAL(5,2),
    normal_max DECIMAL(5,2)
);

CREATE TABLE IF NOT EXISTS public.weather_precipitation
(
    id     SERIAL PRIMARY KEY,
    date VARCHAR(1024),
    precipitation DECIMAL(5,3),
    precipitation_normal DECIMAL(5,3)
);

CREATE TABLE IF NOT EXISTS public.attributes
(
    id     SERIAL PRIMARY KEY,
    business_id VARCHAR(1024),
    AcceptsInsurance VARCHAR,
    AgesAllowed VARCHAR,
    Alcohol VARCHAR,
    Ambience VARCHAR,
    BYOB VARCHAR,
    BYOBCorkage VARCHAR,
    BestNights VARCHAR,
    BikeParking VARCHAR,
    BusinessAcceptsBitcoin VARCHAR,
    BusinessAcceptsCreditCards VARCHAR,
    BusinessParking VARCHAR,
    ByAppointmentOnly VARCHAR,
    Caters VARCHAR,
    CoatCheck VARCHAR,
    Corkage VARCHAR,
    DietaryRestrictions VARCHAR,
    DogsAllowed VARCHAR,
    DriveThru VARCHAR,
    GoodForDancing VARCHAR,
    GoodForKids VARCHAR,
    GoodForMeal VARCHAR,
    HairSpecializesIn VARCHAR,
    HappyHour VARCHAR,
    HasTV VARCHAR,
    Music VARCHAR,
    NoiseLevel VARCHAR,
    Open24Hours VARCHAR,
    OutdoorSeating VARCHAR,
    RestaurantsAttire VARCHAR,
    RestaurantsCounterService VARCHAR,
    RestaurantsDelivery VARCHAR,
    RestaurantsGoodForGroups VARCHAR,
    RestaurantsPriceRange2 VARCHAR,
    RestaurantsReservations VARCHAR,
    RestaurantsTableService VARCHAR,
    RestaurantsTakeOut VARCHAR,
    Smoking VARCHAR,
    WheelchairAccessible VARCHAR,
    WiFi VARCHAR,
    FOREIGN KEY (business_id) REFERENCES public.business(id)
);
