# social-housing-and-wellbeing-improvement
A repeat of the analysis on the effect of social housing on overall well-being of individuals with improved link rates and an additional wave of the GSS survey.

## Overview
This analysis repeats and improves on previous work by the Social Investment Agency (SIA) to measure how placement in social housing impacts the wellbeing of people. In November 2018 we released a report **Measuring the wellbeing impacts of public policy: social housing**, and the accompanying GitHub repository **social-housing-and-wellbeing**. The analysis behind this previous report has been repeated with several improvements, including:

* Wellbeing indicators from the 2016 wave of the New Zealand General Social Survey (NZGSS) are used following its addition into the Integrated Data Infrastructure (IDI).
* Refinements to the spine linking process, using address informaiton to resolve orphan identities, provide a higher link rate for the NZGSS respondents.

Both improvements increase the sample size, reducing uncertainty, and making the results and conclusions more robust. The code from this repositry relates to the SIA report **Measuring the impact of social housing placement on wellbeing: Evidence from linked survey and administrative data**.

## Dependencies
It is necessary to have an IDI project if you wish to run the code. Visit the Stats NZ website for more information about this. This analysis has been developed for the IDI_Clean_20181020 refresh of the IDI. As changes in database structure can occur between refreshes, access to, and use of, this refresh should be considered a dependency for the purpose of executing the code as it was intended.

Code dependencies for this repositry may include the `social_investment_analytical_layer (SIAL)`, the `social_investment_data_foundation (SIDF)`, and the `SIAtoolbox`. Of these, only the SIAL would need to be run prior to this analysis. Please see the SIAL repositry for more detail and instructions. Any dependency on the SIDF and tool box are via SAS macros and R functions contained in those repositries.

## Folder descriptions
This repositry contains all the core code to assemble the data and run the analysis.

* **sasautos:** This folder contains SAS macros that are used during the processing.
* **sasprogs:** This folder contains SAS programs. The main script that builds the dataset is located in here as well as the control file where analysis parameters are entered.
* **rprogs:** This folder contains all the necessary R scripts that are required to perform the analysis on the dataset created by the SAS code.
* **sql:** Several auxiliary SQL scripts that create summary outputs for the final report are stored here.

## Instructions to run the project
### Step A: Create analysis population and variables
1. Start a new SAS session
2. Open `sasprogs/si_control.sas`. Go to the yellow datalines and update any of the parameters that need changing. The one that is most likely to change if you are outside the SIA is the `si_proj_schema`. In case the IDI version that you're pointing to needs to be updated, edit the corresponding variables as well- the variables are `idi_version` and `si_idi_dsnname`. Note that the results in the paper are based on IDI_Clean_20181020. If you have made changes save and close the file.
3. Open `sasprogs/si_main.sas` and change the `si_source_path` variable to your project folder directory. Once this is done, run the `si_main.sas` script, which will build the datasets that are needed to do the analysis.

### Step B: Data Preparation & Analysis
There are 2 distinct streams of analysis for this project-

**Survey-Weighted Descriptive Statistics**
1. Start a new R session.

2. Open up `rprogs/1_of_weighted_gss_analysis_wrapper.R`. Edit the working directory by modifying with path specified at the first line of this file. In addition, also edit the variable `schemaname` to the appropriate schema that you are using. This is a wrapper script that runs all steps involved for generating the weighted descriptive statistics for the analysis. The script performs a linking of the GSS survey data with the IDI Spine, and reweights the survey to account for records that are unlinked with the IDI Spine. A comparison test between the distribution of the GSS variables is also performed pre and post-reweighting. This is to ensure that the IDI Spine linkage and the subsequent and re-weighting procedure does not bias the variables that is to be compared. The outputs of this operation can be obtained from the `output` folder. 

**Unweighted Before-After Analysis**
1. Start a new R session.
2. Open up `rprogs/1_run_analysis_treat_control.R`. This script creates loads up all required libraries and generates all the Before-After analysis results. This analysis does not take into account the survey weights, and compares the group that was housed 12 months before GSS interview to the group housed 15 months after. Bootstrap sampling is used to get confidence intervals around the estimates here. In addition to the main analysis, this code also performs a validation, by comparing the group that was housed 12 months before GSS interview to the group housed 12 months after, and another validation using propensity matched groups.  Additionally, this code also performs regression models for the outcome variables of interest. The outputs of this analysis can be obtained from the `output` folder. 

## Getting Help
If you have any questions email info@sia.govt.nz

