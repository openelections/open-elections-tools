CREATE TABLE national_voting_data (
    `state` VARCHAR(256),
    `year` INT,
    `date` DATETIME,
    `election` VARCHAR(256),
    `special` BOOLEAN,
    `office` VARCHAR(256),
    `district` VARCHAR(256),
    `county` VARCHAR(256),
    `precinct` VARCHAR(256),
    `party` VARCHAR(256),
    `candidate` VARCHAR(256),
    `votes` INT,
    PRIMARY KEY (`state`, `year`, `date`, `election`, `special`, `office`, `district`, `county`, `precinct`, `party`, `candidate`)
)