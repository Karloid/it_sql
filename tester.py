import queue
import re
import subprocess
import threading

import matplotlib.animation as animation
import matplotlib.pyplot as plt

# Define a thread-safe queue for data sharing
data_queue = queue.Queue()


indexOfRunnerToWatch = 943241
threadIndex = [indexOfRunnerToWatch, 11331, 29922, 399333, 4945345, 539312]

# Define a function to run the command
def run_command(index):
    print("started match for seed " + str(index))
    oppFile = 'solution_08_bug_fixes.sql'
    command = 'docker run --rm -i -e SEED=' + str(
        #       index) + ' --mount "type=bind,src=$(pwd)/options.toml,dst=/tmp/options.toml" --mount "type=bind,src=$(pwd)/solution.sql,dst=/tmp/player1.sql" --mount "type=bind,src=$(pwd)/solution_04.sql,dst=/tmp/player2.sql" ghcr.io/all-cups/it_one_cup_sql --solution /tmp/player1.sql --solution /tmp/player2.sql --options /tmp/options.toml'
        index) + ' --mount "type=bind,src=$(pwd)/R2-options.toml,dst=/tmp/options.toml" --mount "type=bind,src=$(pwd)/solution.sql,dst=/tmp/player1.sql" --mount "type=bind,src=$(pwd)/' + oppFile + ',dst=/tmp/player2.sql" ghcr.io/all-cups/it_one_cup_sql --solution /tmp/player1.sql --solution /tmp/player2.sql --options /tmp/options.toml'

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

        if index != indexOfRunnerToWatch:
            continue

        # print("o:" + decodedLine)

        # Use regular expressions to extract time and money
        time_match = re.search(r"time:\s(-?[\d.]+)", decodedLine)
        money_match = re.search(r"money:\s(-?[\d.]+)", decodedLine)
        opp_match = re.search(r"opp:\s(-?[\d.]+)", decodedLine)

        my_money_per_time_match = re.search(r"myMoneyPerTime=(-?[\d.]+)", decodedLine)
        opp_money_per_time_match = re.search(r"oppMoneyPerTime=(-?[\d.]+)", decodedLine)
        total_contract_qty_match = re.search(r"totalContractQty=([\d.]+)", decodedLine)
        stored_at_customer_qty_match = re.search(r"storedAtCustomerQty=([\d.]+)", decodedLine)
        parked_cargo_qty_match = re.search(r"parkedCargoQty=([\d.]+)", decodedLine)
        moved_cargo_qty_match = re.search(r"movedCargoQty=([\d.]+)", decodedLine)
        total_ship_capacity_match = re.search(r"totalShipCapacity=([\d.]+)", decodedLine)
        capacity_utilisation_match = re.search(r"capacityUtilisation=(-?[\d.]+)", decodedLine)

        # Check if matches were found
        if time_match and money_match and opp_match:
            my_money_per_time = float(my_money_per_time_match.group(1))
            opp_money_per_time = float(opp_money_per_time_match.group(1))
            total_contract_qty = float(total_contract_qty_match.group(1))
            stored_at_customer_qty = float(stored_at_customer_qty_match.group(1))
            parked_cargo_qty = float(parked_cargo_qty_match.group(1))
            moved_cargo_qty = float(moved_cargo_qty_match.group(1))
            total_ship_capacity = float(total_ship_capacity_match.group(1))
            capacity_utilisation = float(capacity_utilisation_match.group(1))

            time = float(time_match.group(1))
            money = float(money_match.group(1))
            oppMoney = float(opp_match.group(1))

            # Print the extracted values
            data_queue.put((index, time, money, oppMoney, my_money_per_time,
                            opp_money_per_time,
                            total_contract_qty,
                            stored_at_customer_qty,
                            parked_cargo_qty,
                            moved_cargo_qty,
                            total_ship_capacity,
                            capacity_utilisation
                            ))
            data_queue.task_done()

    process.wait()


# Create a thread for each seed running the command
for index in threadIndex:
    command_thread = threading.Thread(target=run_command, args=(index,))
    command_thread.start()

# Initialize figure and axes
fig, ax = plt.subplots()

# Initialize empty lists for x and y data
x_data = []
y1_data = []
opp_money_data = []
contracts_qty = []
utilisation = []
stored_at_customer_qty_data = []
cargo_qty_data = []

# Initialize the line plot
linePlot1, = ax.plot(x_data, y1_data, color='blue')
linePlotOppMoney, = ax.plot(x_data, opp_money_data, color='red')
linePlotContractsQty, = ax.plot(x_data, contracts_qty, color='cyan')
linePlotUtilisation, = ax.plot(x_data, utilisation, color='magenta')
linePlotStoredAtCustomerQty, = ax.plot(x_data, stored_at_customer_qty_data, color='#E19898')
linePlotCargoQty, = ax.plot(x_data, cargo_qty_data, color='#4D3C77')



# Function to update the plot periodically
def update_plot(frame):
    #   print("update plot")
    # Get a line of data from the queue
    while True:
        try:
            (index, time, money, oppMoney, my_money_per_time,
             opp_money_per_time,
             total_contract_qty,
             stored_at_customer_qty,
             parked_cargo_qty,
             moved_cargo_qty,
             total_ship_capacity,
             capacity_utilisation
             ) = data_queue.get(timeout=0.1)
            # Append new data point to the lists
            x_data.append(time)
            y1_data.append(money)
            opp_money_data.append(oppMoney)
            contracts_qty.append(total_contract_qty)
            utilisation.append(capacity_utilisation * 100000.0)
            stored_at_customer_qty_data.append(stored_at_customer_qty)
            cargo_qty_data.append(parked_cargo_qty + moved_cargo_qty)

        except queue.Empty:
            break

    if len(x_data) > 0:
        # Update the line data
        linePlot1.set_data(x_data, y1_data)
        linePlotOppMoney.set_data(x_data, opp_money_data)
        linePlotContractsQty.set_data(x_data, contracts_qty)
        linePlotUtilisation.set_data(x_data, utilisation)
        linePlotStoredAtCustomerQty.set_data(x_data, stored_at_customer_qty_data)
        linePlotCargoQty.set_data(x_data, cargo_qty_data)

        # Set plot limits if desired
        ax.set_xlim(min(x_data), max(x_data) + 1)
        ax.set_ylim(min(min(y1_data), min(opp_money_data)), max(max(y1_data), max(opp_money_data), max(contracts_qty), max(utilisation)) + 1)


# Start the plot update thread
# Create animation
ani = animation.FuncAnimation(fig, update_plot, frames=range(10), interval=2000)

# Display the plot
plt.show()

# Wait for the command and plot update threads to finish
command_thread.join()
