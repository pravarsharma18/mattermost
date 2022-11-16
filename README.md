Scopes= ZohoProjects.users.ALL,ZohoProjects.portals.ALL,ZohoProjects.projects.ALL,ZohoProjects.tasks.ALL,ZohoProjects.tasklists.ALL,ZohoProjects.tags.ALL,ZohoProjects.users.CREATE,ZohoCliq.Chats.READ,ZohoCliq.Channels.READ,ZohoCliq.Teams.READ,ZohoCliq.Messages.READ,ZohoPeople.forms.READ,ZohoPeople.employee.READ,ZohoPeople.attendance.READ,ZohoCliq.Departments.READ,ZohoPeople.employee.READ,ZohoCliq.Attachments.READ

```
from- Trootech id
https://accounts.zoho.in/oauth/v2/auth?scope=ZohoProjects.users.ALL,ZohoProjects.portals.ALL,ZohoProjects.projects.ALL,ZohoProjects.tasks.ALL,ZohoProjects.tasklists.ALL,ZohoProjects.tags.ALL,ZohoProjects.users.CREATE,ZohoCliq.Chats.READ,ZohoCliq.Channels.READ,ZohoCliq.Teams.READ,ZohoCliq.Messages.READ,ZohoPeople.forms.READ,ZohoPeople.employee.READ,ZohoPeople.attendance.READ,ZohoCliq.Departments.READ,ZohoPeople.employee.READ,ZohoCliq.Attachments.READ&client_id=1000.A2TCKMRMVJ0LSCP62AGVCNYV10S4KY&response_type=code&access_type=offline&redirect_uri=http://localhost:8000&prompt=consent
```

```
from- Personal id
https://accounts.zoho.in/oauth/v2/auth?scope=ZohoProjects.portals.All,ZohoProjects.projects.ALL,ZohoProjects.users.READ,ZohoProjects.tasks.ALL,ZohoProjects.tasklists.ALL,ZohoCliq.Chats.READ,ZohoCliq.Channels.CREATE,ZohoCliq.Channels.READ,ZohoCliq.Channels.UPDATE,ZohoCliq.Channels.DELETE,ZohoCliq.Users.READ,ZohoPeople.forms.READ,ZohoPeople.employee.READ,ZohoPeople.attendance.READ,ZohoCliq.OrganizationChats.READ,ZohoCliq.Profile.READ,ZohoCliq.Departments.READ,ZohoPeople.employee.READ,ZohoCliq.Channels.ALL,ZohoCliq.Messages.READ,ZohoCliq.OrganizationMessages.READ,ZohoCliq.OrganizationChannels.READ,ZohoCliq.Attachments.READ&client_id=1000.63766CS3T9B47YJMWN8YRRHF8NE55U&state=5466400890088961855&response_type=code&access_type=offline&redirect_uri=http://localhost:8000
```

```
http://localhost:8000/?code=1000.dbecfd20359311dd63a3733ca9c5bb2c.a4e0dc4c6d993abe75cb659a94a508b7&location=in&accounts-server=https%3A%2F%2Faccounts.zoho.in&
```

```
https://accounts.zoho.in/oauth/v2/token?code=1000.359545d58fe36c12c724e11b3d1c567b.8864592ae3b0fc78cc32be28700f7557&redirect_uri=http://localhost:8000&client_id=1000.A2TCKMRMVJ0LSCP62AGVCNYV10S4KY&client_secret=6c84014b789a1dc1f25181109322631c637cdd7d6f&grant_type=authorization_code
```

```
docker run --name mattermost-preview -d --publish 8065:8065 mattermost/mattermost-preview
```

focalboard_boards + focalboard_board_members
focalboard_blocks type choices = view, card, text, image

```
# final_data.append({
    #     "id": project['id'],
    #     "name": project['name'],
    #     "team_id": "",
    #     "created_by": project['created_by_id'],
    #     "modified_by": project['updated_by_id'],
    #     "type": random.choices(type)[0],
    #     "description": project['description'],
    #     "create_at": self.get_timestamp(project['created_date']),
    #     "update_at": self.get_timestamp(project['updated_date']),
    #     "minimum_role": ""
    # })
```

```
cd /opt/mattermost && sudo -u mattermost ./bin/mattermost
```

### Create Zoho Tables in local DB

```
python3 create_zoho_tables.py
```

### Get Data from zoho API and Insert in Local DB

```
python3 zoho_client.py
```

### Get Insert in Mattermost DB

```
python3 mattermost_client.py
```

### Delete data from Mattermost and Zoho DB.

```
python3 truncate_tables.py
```
