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
            if debugg then
                raise notice '[PLAYER %] acquired contractors info, contractor % island % item % payment sum % qty %',
                    player_id, contractorInfo.contractor, contractorInfo.island, contractorInfo.item,
                    contractorInfo.payment_sum, contractorInfo.quantity;
            end if;
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

                select best_item.id as item_id, contractors.*, best_item.has_not_empty_vendors, best_item.total_profit
                into bestContractDraft
                from (select *
                      from (select *,
                                   (select count(*)
                                    from world.contractors c
                                    where c.item = i.id
                                      and c.type = 'vendor')                                                    vendors,
                                   (select count(*)
                                    from world.contractors c
                                    where c.item = i.id
                                      and c.type = 'customer'
                                      and c.id not in (select ccc.contractor from my.acquired_contractors ccc)) customers,
                                   -- possible profit
                                   (select max(c.price_per_unit)
                                    from world.contractors c
                                    where c.item = i.id
                                      and c.type = 'customer') - (select min(c.price_per_unit)
                                                                  from world.contractors c
                                                                  where c.item = i.id
                                                                    and c.type = 'vendor')                      max_price_diff,

                                   (select sum(c.price_per_unit * c.quantity)
                                    from world.contractors c
                                    where c.item = i.id
                                      and c.type = 'customer'
                                      and c.id not in (select ccc.contractor from my.acquired_contractors ccc)) -
                                   (select sum(c.price_per_unit * c.quantity)
                                    from world.contractors c
                                    where c.item = i.id
                                      and c.type = 'vendor')                                                    total_profit,

                                   (select sum(c.quantity)
                                    from world.contractors c
                                    where c.item = i.id
                                      and c.type = 'vendor'
                                      and c.price_per_unit < (select max(c.price_per_unit)
                                                              from world.contractors c
                                                              where c.item = i.id
                                                                and c.type = 'customer')) >
                                   1.0                                                                          has_not_empty_vendors,
                                   -- find what quantity of items can be actually sold to best customer
                                   (select min(c.quantity)
                                    from world.contractors c
                                    where c.item = i.id
                                      and c.type = 'vendor') * (select max(c.price_per_unit)
                                                                from world.contractors c
                                                                where c.item = i.id
                                                                  and c.type = 'customer')                      max_sum_value


                            from world.items i) d
                      where d.has_not_empty_vendors = true

                      order by d.total_profit desc
                      limit 1) best_item,
                     world.contractors contractors
                where contractors.item = best_item.id
                  and contractors.type = 'customer'
                  and contractors.price_per_unit = (select max(c.price_per_unit)
                                                    from world.contractors c
                                                    where c.item = best_item.id
                                                      and c.type = 'customer'
                                                      and c.id not in (select ccc.contractor from my.acquired_contractors ccc));

                if bestContractDraft is not null then
                    if debugg then
                        raise notice '[PLAYER %] ship % found best contract draft %', player_id, shipWithState.id, to_json(bestContractDraft);
                    end if;


                    for vendor in
                        select * from queryBestVendor(bestContractDraft.item_id, bestContractDraft.price_per_unit)
                        loop
                            --  raise notice '[PLAYER %] vendor % has % items', player_id, vendor.id, vendor.quantity;
                            vendorQtyToBuy := 0.0;

                            vendorQtyToBuy :=
                                    least(bestContractDraft.quantity, vendor.quantity, shipWithState.capacity);

                            if vendorQtyToBuy < 0.0 then
                                if debugg then
                                    raise notice '[PLAYER %] ERROR ship % cannot calc vendorQtyToBuy, very bad contract=% vendor=%',
                                        player_id, shipWithState.id, to_json(bestContractDraft), to_json(vendor);
                                end if;
                            else
                                -- insert to offers contractor and quantity
                                -- notice about buying stuff
                                if 1 = 0 then
                                    raise notice '[PLAYER %] buying % items from vendor % by price % sum-buy-value %',
                                        player_id, vendorQtyToBuy, vendor.id, vendor.price_per_unit, vendorQtyToBuy * vendor.price_per_unit;
                                end if;

                                insert into actions.offers (contractor, quantity)
                                values (vendor.id, vendorQtyToBuy)
                                returning id into tmpInt;
                                insert into my.offers (offer, time) values (tmpInt, currentTime);

                                insert into actions.offers (contractor, quantity)
                                values (bestContractDraft.id, vendorQtyToBuy)
                                returning id into tmpInt;
                                insert into my.offers (offer, time) values (tmpInt, currentTime);
                                call acquireContractor(bestContractDraft.id);

                                insert into actions.ship_moves (ship, destination)
                                values (shipWithState.id, vendor.island);

                                call setState(shipWithState.id, 'moving_to_load', currentTime);
                                call setInt('ship_contractor__' || shipWithState.id, bestContractDraft.id);
                                if vendor.island <> shipWithState.island_coalesce then
                                    call moveToTheNextIsland(player_id, shipWithState.id, vendor.island);
                                end if;
                            end if;

                        end loop;
                else
                    if debugg then
                        raise notice '[PLAYER %] ship % no best contract draft found :(', player_id, shipWithState.id;
                    end if;
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
                    insert into actions.wait (until) values (currentTime + 0.1);

                    -- if nothing found thats really bad
                else
                    -- check if enough in storage
                    select coalesce(sum(quantity), 0.0) from world.storage where item = contractor.item into storageQty;

                    if storageQty < contract.quantity then

                        select *
                        into vendor
                        from world.contractors c
                        where c.type = 'vendor'
                          and c.item = contractor.item
                          and c.quantity >= contract.quantity
                        order by c.price_per_unit asc
                        limit 1;

                        if vendor is null then
                            if debugg then
                                raise notice '[PLAYER %] WARN ship % contractorId % cannot find vendor, back to idle',
                                    player_id, shipWithState.id, contractorId;
                            end if;
                            call setState(shipWithState.id, 'idle', currentTime);
                            insert into actions.wait (until) values (currentTime + 0.1);

                        else
                            vendorQtyToBuy := 0.0;

                            vendorQtyToBuy := contract.quantity;

                            if vendorQtyToBuy < 0.0 then
                                if debugg then
                                    raise notice '[PLAYER %] !ERROR! ship % cannot calc vendorQtyToBuy, very bad contract=% vendor=%',
                                        player_id, shipWithState.id, to_json(contract), to_json(vendor);
                                end if;
                            else

                                insert into actions.offers (contractor, quantity)
                                values (vendor.id, vendorQtyToBuy)
                                returning id into tmpInt;
                                insert into my.offers (offer, time) values (tmpInt, currentTime);

                                insert into actions.ship_moves (ship, destination)
                                values (shipWithState.id, vendor.island);

                                call setState(shipWithState.id, 'moving_to_load', currentTime);
                                if vendor.island <> shipWithState.island_coalesce then
                                    call moveToTheNextIsland(player_id, shipWithState.id, vendor.island);
                                end if;
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
                        contract.quantity,
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