CREATE TABLE "parent1" ( "id" INT NOT NULL, "update_ts" TIMESTAMP DEFAULT NULL, "in_ts" TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY ("id") );
CREATE TABLE "child11" ( "child_id" INT NOT NULL, "parent_id" INT, "update_ts" TIMESTAMP DEFAULT NULL, "in_ts" TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY ("child_id"), FOREIGN KEY ("parent_id") REFERENCES "parent1"("id") );
CREATE INDEX "par_ind" ON "child11"("parent_id");
CREATE TABLE "parent2" ( "id" INT NOT NULL, "update_ts" TIMESTAMP DEFAULT NULL, "in_ts" TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY ("id") );
CREATE TABLE "child21" ( "child_id" INT NOT NULL, "parent_id" INT, "update_ts" TIMESTAMP DEFAULT NULL, "in_ts" TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY ("child_id"), FOREIGN KEY ("parent_id") REFERENCES "parent2"("id") );
CREATE INDEX "par_ind_5" ON "child21"("parent_id");
CREATE TABLE "child31" ( "child_id" INT NOT NULL, "parent_id" INT, "update_ts" TIMESTAMP DEFAULT NULL, "in_ts" TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY ("child_id"), FOREIGN KEY ("parent_id") REFERENCES "parent2"("id") );
CREATE INDEX "par_ind_6" ON "child31"("parent_id");
