<h1>1. Introduction</h1>

The final project made for the <a href="https://github.com/DataTalksClub/data-engineering-zoomcamp">DataTalksClub/data-engineering-zoomcamp</a> This document provides explanation to the installation process and is 
seperated into several sections depending on the tools used.


<h3>Data Used</h3>

Data used is the rotten-tomatoes user and critics review.

<a href="https://components.one/datasets/film-reviews-208000-critic-reviews-and-10-7-million-user-reviews"> Link to website </a>

<h3>Important Links</h3>
<a href="https://lookerstudio.google.com/reporting/ff15f220-0c6c-4e90-a30e-79c9a998874d">Project visualization link</a>

<h3>Tools</h3>

To be able to run this project you need to have the have following software installed.

<ul>
<li>Docker</li>
<li>Terraform</li>
<li>Google Cloud Platform Account</li>
</ul>


<h1>2. Installation Steps</h1>

<ol>
<li>Clone this reposatory</li>
<li>Create the folder named keys/ under the rotten-tomatoes-datacamp-project/dags/
<li>Create a Google Cloud Platform Account (GCP): <a href="https://cloud.google.com/"> here</a></li>
<ol>
<li>Create a service account by following the steps <a href="https://cloud.google.com/iam/docs/service-accounts-create"> here</a></li>
<li>Create and export a service account key by following the steps<a href="https://cloud.google.com/iam/docs/keys-create-delete"> here</a></li>
<li>Download the created service account key, rename it to gcp-cred.json and move it under rotten-tomatoes-datacamp-project/dags/keys/
</ol>
<li>Creating the infrastructure with Terraform</li>
<ol>
<li>Using the terminal move into rotten-tomatoes-datacamp-project/terraform folder</li>
<li>Run the command: terraform init </li>
<li>Run the command: terraform apply </li>
</ol>
<li>Docker</li>
<ol>
<li>Using terminal move into folder rotten-tomatoes-datacamp-project/ </li>
<li>Run the command: docker-compose up airflow-init</li>
<li>Run the command: docker compose up -d</li>
</ol>
<li>Pipeline progress can be tracked <a href="http://localhost:8080/">here</a></li>
<li>Final visualization can be seen <a href="https://lookerstudio.google.com/reporting/ff15f220-0c6c-4e90-a30e-79c9a998874d">here</a></li>
</ol>



<h1>Pipeline Infrastructure</h1>
![Pipeline Infrastructure](https://user-images.githubusercontent.com/72452622/234042403-06fa41da-c5ae-444f-a82c-7b398cd32374.png)











