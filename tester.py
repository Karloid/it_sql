import subprocess
import threading
import matplotlib.pyplot as plt
import queue
import matplotlib.animation as animation
import re

# Define a thread-safe queue for data sharing
data_queue = queue.Queue()


# Define a function to run the command
def run_command(index):
    print("started match for seed " + str(index))
    command = 'docker run --rm -i -e SEED=' + str(
 #       index) + ' --mount "type=bind,src=$(pwd)/options.toml,dst=/tmp/options.toml" --mount "type=bind,src=$(pwd)/solution.sql,dst=/tmp/player1.sql" --mount "type=bind,src=$(pwd)/solution_04.sql,dst=/tmp/player2.sql" ghcr.io/all-cups/it_one_cup_sql --solution /tmp/player1.sql --solution /tmp/player2.sql --options /tmp/options.toml'
        index) + ' --mount "type=bind,src=$(pwd)/R2-options.toml,dst=/tmp/options.toml" --mount "type=bind,src=$(pwd)/solution.sql,dst=/tmp/player1.sql" --mount "type=bind,src=$(pwd)/solution_04.sql,dst=/tmp/player2.sql" ghcr.io/all-cups/it_one_cup_sql --solution /tmp/player1.sql --solution /tmp/player2.sql --options /tmp/options.toml'

    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    firstPlayerScore = None
    for line in process.stderr:
        # Put each line of output in the data queue
        #  print(line.decode().strip())
        decodedLine = line.decode().strip()

        # Extract Player Number and Score using regular expressions
        match = re.search(r"Player #(\d+) = (\d+\.\d+)", decodedLine)
        if match:
            playerNumber = int(match.group(1))
            score = float(match.group(2))

            if playerNumber == 1:
                firstPlayerScore = score
            if playerNumber == 2:
                # print which player won and what is final score
                scoreDiff = firstPlayerScore - score
                if score > firstPlayerScore:
                    print("FINISH: " + str(index) + " player 2 (BAD) won with score " + str(score) + " against " + str(
                        firstPlayerScore) + " diff: " + str(scoreDiff))
                else:
                    print("FINISH: " + str(index) + " player 1 won with score " + str(
                        firstPlayerScore) + " against " + str(score) + " diff: " + str(scoreDiff))
        if index != 1:
            continue

        # print("o:" + decodedLine)

        # Use regular expressions to extract time and money
        time_match = re.search(r"time:\s([\d.]+)", decodedLine)
        money_match = re.search(r"money:\s([\d.]+)", decodedLine)
        opp_match = re.search(r"opp:\s([\d.]+)", decodedLine)

        # Check if matches were found
        if time_match and money_match and opp_match:
            time = float(time_match.group(1))
            money = float(money_match.group(1))
            oppMoney = float(opp_match.group(1))

            # Print the extracted values
            data_queue.put((index, time, money, oppMoney))
            data_queue.task_done()

    process.wait()


threadIndex = [0, 1, 2, 3, 4]

# Create a thread for each seed running the command
for index in threadIndex:
    command_thread = threading.Thread(target=run_command, args=(index,))
    command_thread.start()

# Initialize figure and axes
fig, ax = plt.subplots()

# Initialize empty lists for x and y data
x_data = []
y1_data = []
y2_data = []

# Initialize the line plot
linePlot1, = ax.plot(x_data, y1_data, color='blue')
linePlot2, = ax.plot(x_data, y2_data, color='red')


# Function to update the plot periodically
def update_plot(frame):
    #   print("update plot")
    # Get a line of data from the queue
    while True:
        try:
            (index, time, money, oppMoney) = data_queue.get(timeout=0.1)
            # Append new data point to the lists
            x_data.append(time)
            y1_data.append(money)
            y2_data.append(oppMoney)
        except queue.Empty:
            break

    if len(x_data) > 0:
        # Update the line data
        linePlot1.set_data(x_data, y1_data)
        linePlot2.set_data(x_data, y2_data)

        # Set plot limits if desired
        ax.set_xlim(0, max(x_data) + 1)
        ax.set_ylim(0, max(max(y1_data), max(y2_data)) + 1)


# Start the plot update thread
# Create animation
ani = animation.FuncAnimation(fig, update_plot, frames=range(10), interval=500)

# Display the plot
plt.show()

# Wait for the command and plot update threads to finish
command_thread.join()
