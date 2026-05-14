use std::any::Any;
use std::sync::Arc;

/// Extension trait that makes it easier to work with traits objects that implement [`Any`],
/// implemented automatically for any type that satisfies `Any`, `Send`, and `Sync`. In particular,
/// given some `trait T: Any + Send + Sync`, it allows upcasting `T` to `dyn Any + Send + Sync`,
/// which in turn allows downcasting the result to a concrete type.
///
/// For example, the following code will compile:
///
/// ```
/// # use iceberg_lite::engine::AsAny;
/// # use std::any::Any;
/// # use std::sync::Arc;
/// trait Foo : AsAny {}
/// struct Bar;
/// impl Foo for Bar {}
///
/// let f: Arc<dyn Foo> = Arc::new(Bar);
/// let a: Arc<dyn Any + Send + Sync> = f.as_any();
/// let b: Arc<Bar> = a.downcast().unwrap();
/// ```
///
/// In contrast, very similar code that relies only on `Any` would fail to compile:
///
/// ```fail_compile
/// # use std::any::Any;
/// # use std::sync::Arc;
/// trait Foo: Any + Send + Sync {}
///
/// struct Bar;
/// impl Foo for Bar {}
///
/// let f: Arc<dyn Foo> = Arc::new(Bar);
/// let b: Arc<Bar> = f.downcast().unwrap(); // `Arc::downcast` method not found
/// ```
///
/// As would this:
///
/// ```fail_compile
/// # use std::any::Any;
/// # use std::sync::Arc;
/// trait Foo: Any + Send + Sync {}
///
/// struct Bar;
/// impl Foo for Bar {}
///
/// let f: Arc<dyn Foo> = Arc::new(Bar);
/// let a: Arc<dyn Any + Send + Sync> = f; // trait upcasting coercion is not stable rust
/// let f: Arc<Bar> = a.downcast().unwrap();
/// ```
///
/// NOTE: `AsAny` inherits the `Send + Sync` constraint from [`Arc::downcast`].
pub trait AsAny: Any + Send + Sync {
    /// Obtains a `dyn Any` reference to the object:
    ///
    /// ```
    /// # use iceberg_lite::engine::AsAny;
    /// # use std::any::Any;
    /// # use std::sync::Arc;
    /// trait Foo : AsAny {}
    /// struct Bar;
    /// impl Foo for Bar {}
    ///
    /// let f: &dyn Foo = &Bar;
    /// let a: &dyn Any = f.any_ref();
    /// let b: &Bar = a.downcast_ref().unwrap();
    /// ```
    fn any_ref(&self) -> &(dyn Any + Send + Sync);

    /// Obtains an `Arc<dyn Any>` reference to the object:
    ///
    /// ```
    /// # use iceberg_lite::engine::AsAny;
    /// # use std::any::Any;
    /// # use std::sync::Arc;
    /// trait Foo : AsAny {}
    /// struct Bar;
    /// impl Foo for Bar {}
    ///
    /// let f: Arc<dyn Foo> = Arc::new(Bar);
    /// let a: Arc<dyn Any + Send + Sync> = f.as_any();
    /// let b: Arc<Bar> = a.downcast().unwrap();
    /// ```
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;

    /// Converts the object to `Box<dyn Any>`:
    ///
    /// ```
    /// # use iceberg_lite::engine::AsAny;
    /// # use std::any::Any;
    /// # use std::sync::Arc;
    /// trait Foo : AsAny {}
    /// struct Bar;
    /// impl Foo for Bar {}
    ///
    /// let f: Box<dyn Foo> = Box::new(Bar);
    /// let a: Box<dyn Any> = f.into_any();
    /// let b: Box<Bar> = a.downcast().unwrap();
    /// ```
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync>;

    /// Convenient wrapper for [`std::any::type_name`], since [`Any`] does not provide it and
    /// [`Any::type_id`] is useless as a debugging aid (its `Debug` is just a mess of hex digits).
    fn type_name(&self) -> &'static str;
}

// Blanket implementation for all eligible types
impl<T: Any + Send + Sync> AsAny for T {
    fn any_ref(&self) -> &(dyn Any + Send + Sync) {
        self
    }
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync> {
        self
    }
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}
