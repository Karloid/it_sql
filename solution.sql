--set session game.const.transfer_time_per_unit = 1.0;

CREATE SCHEMA my;
CREATE TABLE "my"."kv_double"
(
    "key"   TEXT             NOT NULL PRIMARY KEY,
    "value" DOUBLE PRECISION NOT NULL
);

CREATE TABLE "my"."kv_int"
(
    "key"   TEXT NOT NULL PRIMARY KEY,
    "value" int  NOT NULL
);

CREATE TABLE "my"."kv_text"
(
    "key"   TEXT NOT NULL PRIMARY KEY,
    "value" int  NOT NULL
);

create table "my"."offers"
(
    "offer" integer          not null,
    "time"  double precision not null
);

CREATE TYPE "my"."ship_state" AS ENUM ('idle', 'moving_to_load', 'wait_for_load_finish', 'moving_to_unload');
create table "my"."ship_states"
(
    "ship"  integer          not null primary key,
    "state" my.ship_state    not null,
    "time"  double precision not null
);

create table "my"."acquired_contractors"
(
    "contractor" integer not null primary key
);

create or replace procedure acquireContractor(contractorId integer) as
$$
declare
    debugg boolean := true;
begin
    if debugg then raise notice 'acquireContractor %', contractorId; end if;
    insert into "my"."acquired_contractors" ("contractor") values (contractorId) on conflict do nothing;
end
$$ language plpgsql;

create or replace function getState(shipId integer) returns my.ship_state as
$$
declare
    result my.ship_state;
begin
    select "state" into result from "my"."ship_states" where "ship" = shipId;
    return result;
end;
$$ language plpgsql;

CREATE OR REPLACE PROCEDURE setState(shipId integer, stateInp my.ship_state, curTime double precision) AS
$$
declare
    debugg boolean := true;
BEGIN
    if debugg then raise notice 'setState % %', shipId, stateInp; end if;
    if stateInp is null then
        delete from "my"."ship_states" where "ship" = shipId;
        return;
    end if;
    INSERT INTO "my"."ship_states" ("ship", "state", "time")
    VALUES (shipId, stateInp, curTime)
    ON CONFLICT ("ship") DO UPDATE SET "state" = EXCLUDED."state", "time" = EXCLUDED."time";
END;
$$ LANGUAGE plpgsql;


CREATE INDEX offers_offer_index
    ON "my"."offers" ("offer");

-- Function to get an integer value by key from "kv_int" table
CREATE OR REPLACE FUNCTION getInt(keyInp TEXT)
    RETURNS int AS
$BODY$
DECLARE
    result int;
BEGIN
    SELECT "value"
    INTO result
    FROM "my"."kv_int"
    WHERE "key" = keyInp;

    RETURN result;
END;
$BODY$
    LANGUAGE plpgsql;

-- Procedure to set an integer value by key in "kv_int" table
CREATE OR REPLACE PROCEDURE setInt(keyInp TEXT, valueInp int) AS
$BODY$
declare
    debugg boolean := true;
BEGIN
    if debugg then raise notice 'setInt % %', keyInp, valueInp; end if;
    if valueInp is null then
        delete from "my"."kv_int" where "key" = keyInp;
        return;
    end if;
    INSERT INTO "my"."kv_int" ("key", "value")
    VALUES (keyInp, valueInp)
    ON CONFLICT ("key") DO UPDATE SET "value" = EXCLUDED."value";
END;
$BODY$
    LANGUAGE plpgsql;

-- Function to get a double value by key from "kv_double" table
CREATE OR REPLACE FUNCTION getDouble(keyInp TEXT)
    RETURNS double precision AS
$BODY$
DECLARE
    result double precision;
BEGIN
    SELECT "value"
    INTO result
    FROM "my"."kv_double"
    WHERE "key" = keyInp;

    RETURN result;
END;
$BODY$
    LANGUAGE plpgsql;

-- Procedure to set a double value by key in "kv_double" table
CREATE OR REPLACE PROCEDURE setDouble(keyInp TEXT, valueInp double precision) AS
$BODY$
declare
    debugg boolean := true;
BEGIN
    if debugg then raise notice 'setDouble % %', keyInp, valueInp; end if;
    if valueInp is null then
        delete from "my"."kv_double" where "key" = keyInp;
        return;
    end if;

    INSERT INTO "my"."kv_double" ("key", "value")
    VALUES (keyInp, valueInp)
    ON CONFLICT ("key") DO UPDATE SET "value" = EXCLUDED."value";
END;
$BODY$
    LANGUAGE plpgsql;

-- Function to get a text value by key from "kv_text" table
CREATE OR REPLACE FUNCTION getText(keyInp TEXT)
    RETURNS text AS
$BODY$
DECLARE
    result text;
BEGIN
    SELECT "value"
    INTO result
    FROM "my"."kv_text"
    WHERE "key" = keyInp;

    RETURN result;
END;
$BODY$
    LANGUAGE plpgsql;

-- Procedure to set a text value by key in "kv_text" table
CREATE OR REPLACE PROCEDURE setText(keyInp TEXT, valueInp text) AS
$BODY$
declare
    debugg boolean := true;
BEGIN
    if debugg then raise notice 'setText % %', keyInp, valueInp; end if;
    if valueInp is null then
        delete from "my"."kv_text" where "key" = keyInp;
        return;
    end if;
    INSERT INTO "my"."kv_text" ("key", "value")
    VALUES (keyInp, valueInp)
    ON CONFLICT ("key") DO UPDATE SET "value" = EXCLUDED."value";
END;
$BODY$
    LANGUAGE plpgsql;


create procedure moveToTheNextIsland(player_id integer, ship_id integer, island_id integer) as
$$
declare
    debugg boolean := true;
begin
    if debugg then raise notice '[PLAYER %] MOVING SHIP % TO ISLAND %', player_id, ship_id, island_id; end if;
    insert into actions.ship_moves (ship, destination) values (ship_id, island_id);
end
$$ language plpgsql;

create function queryBestVendor(item_id integer, price_per_unitInp double precision)
    returns table
            (
                id             integer,
                type           world.contractor_type,
                island         integer,
                item           integer,
                quantity       double precision,
                price_per_unit double precision
            )
as
$$
begin
    return query select *
                 from world.contractors c
                 where c.type = 'vendor'
                   and c.item = item_id
                   and c.price_per_unit <= price_per_unitInp
                   and c.quantity >= 0.0
                 order by c.price_per_unit asc
                 limit 1;
end
$$ language plpgsql;

create function calculateProfit(vendor_id integer,
                                vendor_island integer,
                                vendor_qty double precision,
                                vendor_price_per_unit double precision,
                                customers_id integer,
                                customer_island integer,
                                customer_quantity double precision,
                                customer_price_per_unit double precision,
                                ship_id integer,
                                ship_capacity double precision,
                                ship_speed double precision)
    returns double precision
as
$$
declare
    debugg                   boolean          := true;
    shipInfo                 record;
    customerIsland           record;
    vendorIsland             record;
    shipIsland               record;
    shipToVendorDistance     double precision;
    vendorToCustomerDistance double precision;
    profitPerTime            double precision;
    totalTime                double precision;
    profit                   double precision;
    itemsQuantity            double precision;
    tmpInt                   integer;
    transfer_time_per_unit   double precision := 1.0;
begin
    totalTime := 0.0;

    -- get shipInfo
    select * into shipInfo from world.ships where id = ship_id;
    -- get distance between vendor and ship
    select * into vendorIsland from world.islands where id = vendor_island;
    select * into shipIsland from world.islands where id = (select island from world.parked_ships where ship = ship_id);
    select * into customerIsland from world.islands where id = customer_island;

    shipToVendorDistance := calcDistance(shipIsland.x, shipIsland.y, vendorIsland.x, vendorIsland.y);
    vendorToCustomerDistance := calcDistance(vendorIsland.x, vendorIsland.y, customerIsland.x, customerIsland.y);

    itemsQuantity := least(vendor_qty, customer_quantity, ship_capacity);
    profit := itemsQuantity * (customer_price_per_unit - vendor_price_per_unit);

    totalTime := shipToVendorDistance / ship_speed + vendorToCustomerDistance / ship_speed +
                 itemsQuantity * transfer_time_per_unit * 2;

    profitPerTime := profit / totalTime;
    return profitPerTime;
end;
$$ language plpgsql;

-- При расчете времени перемещения используется манхеттенское расстояние между текущим и целевым островом. При этом игровая карта является цикличной по обеим координатам.
create function calcDistance(x1 double precision, y1 double precision, x2 double precision, y2 double precision)
    returns double precision as
$$
declare
    debugg          boolean := true;
    xDiff           double precision;
    yDiff           double precision;
    map_size_actual double precision;
    result          double precision;
begin
    select map_size into map_size_actual from world.global;

    xDiff := abs(x1 - x2);
    yDiff := abs(y1 - y2);
    if xDiff > map_size_actual / 2 then
        xDiff := map_size_actual - xDiff;
    end if;
    if yDiff > map_size_actual / 2 then
        yDiff := map_size_actual - yDiff;
    end if;
    result := xDiff + yDiff;

    return result;
end
$$ language plpgsql;

CREATE PROCEDURE think(player_id INTEGER)
    LANGUAGE PLPGSQL AS
$$
declare
    new_var           int;
    contractorId      int;
    currentTime       double precision;
    myMoney           double precision;
    oppMoney          double precision;
    ship              record;
    shipWithState     record;
    contract          record;
    contractor        record;
    contractorInfo    record;
    bestContractDraft record;
    vendor            record;
    vendorQtyToBuy    double precision;
    debugg            boolean := true;
    tmpInt            integer;
    storageQty        double precision;
    cargoQty          double precision;
    profitInfo        record;

BEGIN
    select game_time into currentTime from world.global;
    select money into myMoney from world.players where id = player_id;
    select money into oppMoney from world.players where id <> player_id order by id limit 1;
    if debugg then
        raise notice '[PLAYER %]      time: % and money: % opp: %', player_id, currentTime, myMoney, oppMoney;

        select count(*) into tmpInt from world.contracts con where con.player = player_id;
        raise notice '[PLAYER %] current_contracts count %', player_id, tmpInt;

        select count(*) into tmpInt from events.contract_completed contract_completed;
        raise notice '[PLAYER %] contract_completed count %', player_id, tmpInt;

        select count(*) into tmpInt from events.offer_rejected contractRejected;
        raise notice '[PLAYER %] offer_rejected count %', player_id, tmpInt;

        select count(*) into tmpInt from events.contract_started contractStarted;
        raise notice '[PLAYER %] contract_started count %', player_id, tmpInt;
    end if;

    -- handle initial setup of states
    if not exists (select * from my.ship_states) then
        if debugg then raise notice '[PLAYER %] setting initial state for ships', player_id; end if;
        for ship in select * from world.ships where player = player_id
            loop
                call setState(ship.id, 'idle', currentTime);
            end loop;
    end if;

    -- handle acquired customer contractors
    -- delete all acquired contractors
    delete from my.acquired_contractors;


    for contract in select * from world.contracts where player = player_id
        loop
            insert into my.acquired_contractors (contractor) values (contract.contractor);
        end loop;

    -- print acquired contractors + contracts
    for contractorInfo in select ac.contractor, c.island, c.item, con.payment_sum, con.quantity
                          from my.acquired_contractors ac,
                               world.contractors c
                                   join world.contracts con on con.contractor = c.id and con.player = player_id
                          where ac.contractor = c.id
        loop
            /*   if debugg then
                   raise notice '[PLAYER %] acquired contractors info, contractor % island % item % payment sum % qty %',
                       player_id, contractorInfo.contractor, contractorInfo.island, contractorInfo.item,
                       contractorInfo.payment_sum, contractorInfo.quantity;
               end if;*/
        end loop;


    -- print current state of ships
    for shipWithState in
        select s.*,
               ss.state,
               coalesce(ps.island, ts.island) island_coalesce,
               cargo.item,
               cargo.quantity,
               case
                   when ps.island is not null then 'parked'
                   when ts.island is not null then 'transferring'
                   when ms.start is not null then 'moving from ' || ms.start || ' to ' || ms.destination
                   else 'unknown!'
                   end as                     game_state

        from world.ships s
                 left join my.ship_states ss on ss.ship = s.id
                 left join world.cargo cargo on s.id = cargo.ship
                 left join world.transferring_ships ts on s.id = ts.ship
                 left join world.parked_ships ps on s.id = ps.ship
                 left join world.moving_ships ms on s.id = ms.ship
        where s.player = player_id
        order by s.id
        loop
            if debugg then
                raise notice '[PLAYER %] ship % state % island % contains item % qty % game_state %',
                    player_id, shipWithState.id, shipWithState.state, shipWithState.island_coalesce,
                    shipWithState.item, shipWithState.quantity, shipWithState.game_state;
            end if;
        end loop;

    -- state machine
    for shipWithState in
        select s.*,
               ss.state,
               coalesce(ps.island, ts.island) island_coalesce,
               cargo.item,
               cargo.quantity,
               case
                   when ps.island is not null then 'parked'
                   when ts.island is not null then 'transferring'
                   when ms.start is not null then 'moving from ' || ms.start || ' to ' || ms.destination
                   else 'unknown!'
                   end as                     game_state

        from world.ships s
                 left join my.ship_states ss on ss.ship = s.id
                 left join world.cargo cargo on s.id = cargo.ship
                 left join world.transferring_ships ts on s.id = ts.ship
                 left join world.parked_ships ps on s.id = ps.ship
                 left join world.moving_ships ms on s.id = ms.ship
        where s.player = player_id
          and ps.island is not null
        order by s.id
        loop
            if debugg then
                raise notice '[PLAYER %] handle parked states ship % state % island % contains item % qty % game_state %',
                    player_id, shipWithState.id, shipWithState.state, shipWithState.island_coalesce,
                    shipWithState.item, shipWithState.quantity, shipWithState.game_state;
            end if;

            -- handle ship states
            if shipWithState.state = 'idle' then
                -- TODO calculate best based on speed\transfer time etc...

                /*    for profitInfo in
                        select vendors.id                                                          vendorId,
                               customers.id                                                        customerId,
                               calculateProfit(
                                       vendors.id,
                                       vendors.island,
                                       vendors.quantity,
                                       vendors.price_per_unit,
                                       customers.id,
                                       customers.island,
                                       customers.quantity,
                                       customers.price_per_unit,
                                       shipWithState.id,
                                       shipWithState.capacity,
                                       shipWithState.speed
                                   )                                                               profitPerTime,
                               least(vendors.quantity, customers.quantity, shipWithState.capacity) finalQuantity
                        from world.contractors vendors,
                             world.contractors customers
                        where vendors.item = customers.item
                          and vendors.type = 'vendor'
                          and customers.type = 'customer'
                          and customers.id not in (select ac.contractor from my.acquired_contractors ac)
                        order by profitPerTime desc
                        loop
                            if debugg then
                                raise notice '[PLAYER %] profitCalc vendor % customer % profitPerTime % finalQuantity %',
                                    player_id, profitInfo.vendorId, profitInfo.customerId, profitInfo.profitPerTime,
                                    profitInfo.finalQuantity;
                            end if;
                        end loop;*/

                select vendors.id                                                          vendorId,
                       customers.id                                                        customerId,
                       vendors.island                                                      vendorIsland,
                       least(vendors.quantity, customers.quantity, shipWithState.capacity) finalQuantity,
                       calculateProfit(
                               vendors.id,
                               vendors.island,
                               vendors.quantity,
                               vendors.price_per_unit,
                               customers.id,
                               customers.island,
                               customers.quantity,
                               customers.price_per_unit,
                               shipWithState.id,
                               shipWithState.capacity,
                               shipWithState.speed
                           )                                                               profitPerTime
                into profitInfo
                from world.contractors vendors,
                     world.contractors customers
                where vendors.item = customers.item
                  and vendors.type = 'vendor'
                  and customers.type = 'customer'
                  and customers.id not in (select ac.contractor from my.acquired_contractors ac)
                order by profitPerTime desc
                limit 1;

                if profitInfo.profitPerTime > 0.0 then
                    if debugg then
                        raise notice '[PLAYER %] ship % take contract to work %',
                            player_id, shipWithState.id, to_json(profitInfo);
                    end if;

                    insert into actions.offers (contractor, quantity)
                    values (profitInfo.vendorId, profitInfo.finalQuantity)
                    returning id into tmpInt;
                    insert into my.offers (offer, time) values (tmpInt, currentTime);

                    insert into actions.offers (contractor, quantity)
                    values (profitInfo.customerId, profitInfo.finalQuantity)
                    returning id into tmpInt;
                    insert into my.offers (offer, time) values (tmpInt, currentTime);
                    call acquireContractor(profitInfo.customerId);

                    call setState(shipWithState.id, 'moving_to_load', currentTime);
                    call setInt('ship_contractor__' || shipWithState.id, profitInfo.customerId);
                    if profitInfo.vendorIsland <> shipWithState.island_coalesce then
                        call moveToTheNextIsland(player_id, shipWithState.id, profitInfo.vendorIsland);
                    end if;

                else
                    -- wait for offers
                    insert into actions.wait (until) values (currentTime + 0.001);
                end if;


            elseif shipWithState.state = 'moving_to_load' then
                -- ok so, items shoulds be available at the island otherwise we should reacquire items

                contractorId := getInt('ship_contractor__' || shipWithState.id);

                select * from world.contractors contractors where contractors.id = contractorId into contractor;
                select * from world.contracts contracts where contracts.contractor = contractorId into contract;

                if contract is null then
                    if debugg then
                        raise notice '[PLAYER %] WARN ship % contract % not found, back to idle', player_id, shipWithState.id, contractorId;
                    end if;
                    call setState(shipWithState.id, 'idle', currentTime);
                    insert into actions.wait (until) values (currentTime + 0.001);

                    -- if nothing found thats really bad
                else
                    -- check if enough in storage
                    select coalesce(sum(quantity), 0.0) from world.storage where item = contractor.item into storageQty;

                    if storageQty < contract.quantity then

                        select vendors.id                                                          vendorId,
                               customers.id                                                        customerId,
                               vendors.island                                                      vendorIsland,
                               least(vendors.quantity, customers.quantity, shipWithState.capacity) finalQuantity,
                               calculateProfit(
                                       vendors.id,
                                       vendors.island,
                                       vendors.quantity,
                                       vendors.price_per_unit,
                                       customers.id,
                                       customers.island,
                                       customers.quantity,
                                       customers.price_per_unit,
                                       shipWithState.id,
                                       shipWithState.capacity,
                                       shipWithState.speed
                                   )                                                               profitPerTime

                        into profitInfo
                        from world.contractors vendors,
                             world.contractors customers
                        where vendors.item = customers.item
                          and vendors.type = 'vendor'
                          and customers.id = contractorId
                          and customers.type = 'customer'
                        --and customers.id not in (select ac.contractor from my.acquired_contractors ac)
                        order by profitPerTime desc
                        limit 1;

                        if profitInfo is null or profitInfo.profitPerTime < 0.0 then
                            if debugg then
                                raise notice '[PLAYER %] !ERROR! ship % contractorId % cannot find profitInfo + vendor, back to idle',
                                    player_id, shipWithState.id, contractorId;
                            end if;
                            call setState(shipWithState.id, 'idle', currentTime);
                            insert into actions.wait (until) values (currentTime + 0.1);

                        else
                            insert into actions.offers (contractor, quantity)
                            values (profitInfo.vendorId, profitInfo.finalQuantity)
                            returning id into tmpInt;
                            insert into my.offers (offer, time) values (tmpInt, currentTime);

                            call setState(shipWithState.id, 'moving_to_load', currentTime);
                            if profitInfo.vendorIsland <> shipWithState.island_coalesce then
                                call moveToTheNextIsland(player_id, shipWithState.id, profitInfo.vendorIsland);
                            end if;

                        end if;
                    else
                        -- ok, we have enough items in storage, lets load them
                        -- notice about loading stuff
                        if debugg then
                            raise notice '[PLAYER %] ship % loading % items from storage by payment_sum %',
                                player_id, shipWithState.id, contract.quantity, contract.payment_sum;
                        end if;
                        insert into actions.transfers (ship, item, quantity, direction)
                        values (shipWithState.id,
                                contractor.item,
                                contract.quantity,
                                'load');
                        call setState(shipWithState.id, 'wait_for_load_finish', currentTime);
                    end if;
                end if;

            elseif shipWithState.state = 'wait_for_load_finish' then
                contractorId := getInt('ship_contractor__' || shipWithState.id);

                select * from world.contractors contractors where contractors.id = contractorId into contractor;
                select * from world.contracts contracts where contracts.contractor = contractorId into contract;

                -- check if enough qty in cargo
                select coalesce(sum(quantity), 0.0)
                from world.cargo cargo
                where cargo.ship = shipWithState.id
                  and cargo.item = contractor.item
                into cargoQty;

                if cargoQty < contract.quantity then
                    if debugg then
                        raise notice '[PLAYER %] WARN ship % not enough items in cargo, back to ''moving_to_load''', player_id, shipWithState.id;
                    end if;
                    call setState(shipWithState.id, 'moving_to_load', currentTime);
                    insert into actions.wait (until) values (currentTime + 0.1);
                else
                    -- ok, we have enough items in cargo, lets go to destination
                    call moveToTheNextIsland(player_id, shipWithState.id, contractor.island);
                    call setState(shipWithState.id, 'moving_to_unload', currentTime);
                end if;

            elseif shipWithState.state = 'moving_to_unload' then
                contractorId := getInt('ship_contractor__' || shipWithState.id);

                select * from world.contractors contractors where contractors.id = contractorId into contractor;
                select * from world.contracts contracts where contracts.contractor = contractorId into contract;

                insert into actions.transfers (ship, item, quantity, direction)
                values (shipWithState.id,
                        contractor.item,
                        shipWithState.capacity,
                        'unload');

                call setState(shipWithState.id, 'idle', currentTime);
            else
                if debugg then
                    raise notice '[PLAYER %] ship % unknown and unhandled state %', player_id, shipWithState.id, shipWithState.state;
                end if;
            end if;

        end loop;
END
$$;