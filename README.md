# OpenWebTextAsync

This implementation builds on the work of [jcpeterson et al](https://github.com/jcpeterson/openwebtext) to focus on one thing in particular: downloading the html contents as fast as possible. This is achieved through asynchronous scraping in parallel across multiple workers, using only firebase for coordination.

### Dependencies
If you use pipenv (`pip install --user pipenv`), cd to the project root and run
```
pipenv install 
pipenv shell
```
Otherwise, just run the following in a new virtual environment
```
pip3 install -r requirements.txt
```

### Requirements
* Download the pre-filtered URLs [here](https://mega.nz/#F!EZZD0YwJ!9_PlEQzdMVLaNdKv_ICNVQ) and concatenate all into "urls.txt"
* Google Drive storage (preferably at least 40GB)
* Many servers for scraping (preferably with fast internet download speed)
* Firebase project with realtime database set up


### Original OpenAI project links
* Blog Post [(Better Language Models and Their Implications)](https://blog.openai.com/better-language-models/)
* Paper [(Language Models are Unsupervised Multitask Learners)](https://d4mucfpksywv.cloudfront.net/better-language-models/language_models_are_unsupervised_multitask_learners.pdf)
* Code [(https://github.com/openai/gpt-2)](https://github.com/openai/gpt-2)
