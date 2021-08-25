from polly.jobs import Jobs

COOKIE = "cookie_value"
PROJECT_ID = 7311

jobs_client = Jobs(COOKIE)

# Set project to work on
jobs_client.set_project(PROJECT_ID)

all_jobs = jobs_client.get_all_jobs()
print(all_jobs)
num_jobs = len(all_jobs['data'])
print(f"Number of currently running jobs: {num_jobs}")

# Create job
new_job = jobs_client.create_job()
print(new_job)

new_job_id = new_job['data']['job_id']

# Get data for new job
new_job_data = jobs_client.get_job(job_id=new_job_id)
print(new_job_data)

# Cancel the new job
jobs_client.cancel_job(new_job_id)
