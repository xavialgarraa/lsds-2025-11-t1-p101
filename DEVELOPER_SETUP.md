# Developer setup

Follow this guide to set up your laptop with all the tools you need for the LSDS course.


## Office hours
If you get stuck and need help, come to the office hours:
- [07/01/2025 17:00 - 18:00 CET](meet.google.com/jcx-ahty-iis)
- [09/01/2025 17:00 - 18:00 CET](meet.google.com/idb-zdbt-xuc)
- [10/01/2025 17:00 - 18:00 CET](meet.google.com/xce-zhjb-qjz)

## Installation

### IDE

- [Install VSCode](https://code.visualstudio.com/download)

### Version Control

- [Install GitHub Desktop](https://desktop.github.com/download/)

### Docker

- [Install Docker Desktop](https://www.docker.com/products/docker-desktop/)

### Operating System

- If you use Windows, [install WSL](https://canonical-ubuntu-wsl.readthedocs-hosted.com/en/latest/guides/install-ubuntu-wsl2/).

### Configure VSCode

- If you use Windows, [install the WSL extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-wsl) and open VSCode in WSL to follow the next steps. (Optional)
- [Install the Python extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python)
- [Install the Markdown Preview Mermaid Support extension](https://marketplace.visualstudio.com/items?itemName=bierner.markdown-mermaid)
- [Install the Markdown Alert extension](https://marketplace.visualstudio.com/items?itemName=yahyabatulu.vscode-markdown-alert)
- [Install the Black Formatter extension](https://marketplace.visualstudio.com/items?itemName=ms-python.black-formatter)
- Only if you use Windows + WSL, [change the default terminal in VSCode to WSL](https://stackoverflow.com/questions/44435697/change-the-default-terminal-in-visual-studio-code)

### Python

> [!WARNING]
> If you use WSL, remember to run these commands in the WSL terminal.

- Verify if you have Python installed

    ```zsh
    $ python3 --version
    Python 3.10.12
    ```

- If you don't, [install Python](https://www.python.org/downloads/)


### JQ

> [!WARNING]
> If you use WSL, remember to run these commands in the WSL terminal.

- [Install jq](https://jqlang.github.io/jq/download/)

## Accept invitations

### Discord

- Join the Discord server (link in Aula Global) where you can ask professors and other students for help in the labs and seminars.

- Mute the channels from the other subjects so you only receive notifications for LSDS. [Help](https://support.discord.com/hc/en-us/articles/209791877-How-do-I-mute-and-disable-notifications-for-specific-channels)

### AWS

- Accept the invitation to AWS academy you received in the email.

## Creating your group's repository

> [!NOTE]
> All (3) members of your group must be from the same lab.
> Only one member of the group must create the repository and share it with the others.

- Fill in [this spreadsheet](https://docs.google.com/spreadsheets/d/1dZ6m6r7b2M4QpjbJlqoQ2XpHhlhRuUGJauw2goXzXto/edit?usp=sharing) with your group members.

- Click the [Use this template -> Create a new repository](https://github.com/miquelvir/lsds-2025) button and create a **private** repository with the following name: `lsds-2025-{group_number}-t{theory_number}-p{lab_number}`. For example, `lsds-2025-01-t1-p102`.

- Share the repository with [all teachers](./TEACHER_LIST.md). [Help](https://docs.github.com/en/account-and-profile/setting-up-and-managing-your-personal-account-on-github/managing-access-to-your-personal-repositories/inviting-collaborators-to-a-personal-repository#inviting-a-collaborator-to-a-personal-repository)

- Share the repository with the other two members of your group. [Help](https://docs.github.com/en/account-and-profile/setting-up-and-managing-your-personal-account-on-github/managing-access-to-your-personal-repositories/inviting-collaborators-to-a-personal-repository#inviting-a-collaborator-to-a-personal-repository)

> [!WARNING]
> If you use WSL, clone your repository to the following path: `\\wsl.localhost\Ubuntu\home\{username}\lsds`.
> 
> Everything inside `\\wsl.localhost` is the file system of your Linux subsystem.

- Clone the repository using Github Desktop. [Help](https://docs.github.com/en/desktop/adding-and-cloning-repositories/cloning-a-repository-from-github-to-github-desktop)

- Fill in the [README](./README.md) with the information about your group.

- Commit and push your changes.

## Download the sample dataset

- Download the file `Eurovision3.json` and `Eurovision9.json` from AulaGlobal and save them inside [projects/2-sparkrdd/data](./projects/2-sparkrdd/data/).

## Making sure Python and Docker work


> [!WARNING]
> If you use WSL, remember to run these commands in the WSL terminal.

- Run a Python server (you can run `deactivate` at any point to exit the venv)

```zsh
cd resources
cd fastapi-quickstart
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
fastapi dev app/main.py
```

- In another terminal, check you can access the server

```zsh
curl -X GET http://127.0.0.1:8000 | jq
```

```zsh
{
  "Hello": "World"
}
```

- Stop the server

- Open Docker Desktop

- Run a Python server with Docker (you can Ctrl-C at any point to stop the containers)

```zsh
cd fastapi-quickstart
docker compose up --build
```

- In another terminal, check you can access the server when it runs in Docker

```zsh
curl -X GET http://127.0.0.1:8001/info | jq
```

```zsh
{
  "studentId": 123,
  "universityName": "upf"
}
```

- Try changing the code in [main.py](./resources/fastapi-quickstart/app/main.py) so it returns `555` as the `studentId`.

- Redeploy the service
```zsh
cd resources
cd fastapi-quickstart
docker compose down
docker compose up --build
```

- Check it works:

```zsh
curl -X GET http://127.0.0.1:8001/info | jq
```

```zsh
{
  "studentId": 555,
  "universityName": "upf"
}
```

## Everything You Should Know

Read and study: [Everything You Should Know](https://docs.google.com/presentation/d/1hVYOn-Cs5LHM8l-t4164iZ7BqfINJoJT/edit?usp=sharing&ouid=102931553666282890148&rtpof=true&sd=true). If you have any questions, come to the Office Hours.
