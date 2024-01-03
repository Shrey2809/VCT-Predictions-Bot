## vctBotBackend

This Python script contains a Discord bot (`vctBotBackend`) designed to manage polls, records, and user stats for VCT-related activities.

### Features
- **Poll Management**: Create and manage polls for VCT matches, allowing users to vote for teams using reactions.
- **Record Keeping**: Record match results, track winners, and manage game statistics.
- **User Stats**: Retrieve user stats and ranks based on their involvement in VCT activities.
- **Leaderboard Generation**: Create top 10 leaderboards for VCT standings.
- **File Handling**: Load specific files, check filenames, and handle file operations.

### Dependencies
- Discord.py: Library for interaction with the Discord API.
- aiohttp: Asynchronous HTTP Client for making web requests.
- pandas: Data manipulation and analysis library.
- tabulate: Package for formatting tabular data.
- Other standard Python libraries (json, random, csv, re).

### Usage
1. **Installation**
   - Install required dependencies using `pip install -r requirements.txt`.
   
2. **Setup**
   - Replace `'etc/emotes.json'` and adjust file paths or names as per your directory structure.
   - Update the bot token in the `config` to match your Discord bot's token.

3. **Running the Bot**
   - Execute the script to start the bot using `python vctBotBackend.py`.

4. **Commands**
   - `+poll <LEAGUE> <TYPE> <DAY>`: Create a poll for a specific VCT match.
   - `+record <Title>`: Record match results based on polls.
   - `+leaderboard`: Generate the top 10 leaderboard for VCT standings.
   - `+score <NAME>`: Retrieve stats for a specific user.
   - Other commands like `+rank`, `+build`, `+check`, `+load`, `+close`, `+setwinner`, `+exit` exist for various functionalities.

### Configuration
- Update `etc/emotes.json` with necessary emojis or emote IDs.
- Adjust the `admin_users` list to grant admin access to specific users.
- Customize logger settings and file paths as required.

### Notes
- The bot utilizes reactions for user input in polls and result recording.
- Ensure appropriate permissions and channel access for the bot in your Discord server.
