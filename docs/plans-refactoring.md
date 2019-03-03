# removing multiple field types / kinds

`schema::SchemaField` vs `db::table::TableField`

This is just getting onerous. To solve we might just
want to combine the schema and db crates together, so that they can
depend on each other. The dependencies here are staggering

The problem is that we want to be able to safely say that a given field exists physically, instead of being synthetic.

```rust
pub struct PhysicalField {
  column: ColumnIdent,
  kind: Kind
}
pub struct SyntheticField {
  alias: Option<Ident>,
  value: Literal | Expr | (ColumnIdent, Kind) // kind is either embedded or can be inferred (hopefully :P)
}

pub enum Field {
  Physical(PhysicalField),
  Synthetic(SyntheticField)
}

impl Into<SyntheticField> for PhysicalField {
  fn into(synth: PhysicalField) -> SyntheticField {
    SyntheticField {
      alias: None,
      value: (synth.column, synth.value)
    }
  }
}

impl Into<SyntheticField> for Field {
  fn into(field: Field) -> SyntheticField {
    match self {
      Field::Physical(phys) => phys.into(),
      Field::Synthetic(syn) => syn
    }
  }
}

impl From<SyntheticField> for Field {
  fn from(synth: SyntheticField) -> Field {
    Field::Synthetic(synth)
  }
}

impl From<PhysicalField> for Field {
  fn from(phys: PhysicalField ) -> Field {
    Field::Physical(phys)
  }
}

```

# removing multiple literal types

db::table::TableFieldLiteral vs parser::LiteralValue

Only one is needed

# making the Row api not bad

It's so unsafe and it's a wonder that things work as well as they do. Need some way of representing the data better, in a way that can still be combined together easily
